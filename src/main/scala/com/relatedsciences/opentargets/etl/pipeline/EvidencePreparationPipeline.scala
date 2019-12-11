package com.relatedsciences.opentargets.etl.pipeline

import java.net.URL
import com.relatedsciences.opentargets.etl.configuration.Configuration.Config
import com.relatedsciences.opentargets.etl.pipeline.JsonValidation.RecordValidator
import com.relatedsciences.opentargets.etl.pipeline.Pipeline.Spec
import com.relatedsciences.opentargets.etl.Common.{UNI_ID_ORG_PREFIX, ENS_ID_ORG_PREFIX}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

case class ValidationResult(isValid: Boolean = false,
                            reason: Option[String] = None,
                            error: Option[String] = None,
                            sourceId: Option[String] = None,
                            dataType: Option[String] = None,
                            targetId: Option[String] = None,
                            diseaseId: Option[String] = None)

case class UniProtGeneLookup(ensembl_gene_id: String, uniprot_id: String)
case class NonReferenceGeneLookup(reference: String, alternate: String)

class EvidencePreparationPipeline(ss: SparkSession, config: Config)
    extends SparkPipeline(ss, config)
    with LazyLogging {
  import ss.implicits._
  import com.relatedsciences.opentargets.etl.pipeline.SparkImplicits._

  // Initialize static schema location from dynamic configuration --
  // is there a better way to do this that still results in a serializable Spark task?
  { RecordValidator.url = Some(new URL(config.evidenceJsonSchema)) }

  lazy val validationResultSchema: StructType =
    ScalaReflection.schemaFor[ValidationResult].dataType.asInstanceOf[StructType]

  def runEvidenceSchemaValidation(df: Dataset[String]): DataFrame = {
    val validatorUdf =
      udf((record: String) => RecordValidator.validate(record), validationResultSchema)
    val dfv = df
      .withColumn("validation", validatorUdf(col("value")))
      .select(col("value"), col("validation.*"))

    // Save bad records as well as the frequencies by source and cause
    this.save(dfv.groupBy("sourceID", "reason").count(), config.evidenceValidationSummaryPath)
    this.save(dfv.filter(!col("isValid")), config.evidenceValidationErrorsPath)

    // Return only valid records and attach hash-based id
    dfv.filter(col("isValid"))
  }

  def parseEvidenceData(df: DataFrame): DataFrame = {
    ss.read.json(df.select("value").as[String])
      // Add source name to unique association fields as the identifier hash input
      // See: https://github.com/opentargets/data_pipeline/blob/329ff219f9510d137c7609478b05d358c9195579/mrtarget/modules/Evidences.py#L190
      .withColumn("id", md5(to_json(struct("unique_association_fields.*", "sourceID"))))
  }

  /**
  * Load Ensembl and UniProt ids such that uniprot ids include all accessions, with ensembl id repeated for each.
    * UniProt ids will be null when not applicable.
    *
    * Approx. 129k rows
    */
  def getUniProtGeneLookup: Dataset[UniProtGeneLookup] = {
    ss.read.json(config.geneDataPath)
      .select($"ensembl_gene_id", explode_outer(
        concat(array($"uniprot_id"), $"uniprot_accessions")
      ).as("uniprot_id"))
      // Convert empty to null to avoid ambiguity post-downstream-join
      .withColumn("uniprot_id", when(trim($"uniprot_id") =!= "", $"uniprot_id").otherwise(lit(null: String)))
      .dropDuplicates()
      .as[UniProtGeneLookup]
  }

  /**
  * Load mapping of "non-reference" genes to genes with an associated reference genome (others correspond to
    * assemblies in highly polymorphic regions, e.g. HLA)
    *
    * Approx. 300 rows
    */
  def getNonReferenceGeneLookup: Dataset[NonReferenceGeneLookup] = {
    ss.read.option("header", "true")
      .csv(config.nonRefGeneDataPath).as[NonReferenceGeneLookup]
  }

  def normalizeTargetIds(df: DataFrame): DataFrame = {
    val dfu = getUniProtGeneLookup.toDF().addPrefix("uniprot:")
    val dfn = getNonReferenceGeneLookup.toDF().addPrefix("nonref:").cache()

    val dfr = df
      // Determine which type of identifier is used based on prefix
      .withColumn("target_id_type",
        when($"target.id".startsWith(ENS_ID_ORG_PREFIX), "ensembl")
          .when($"target.id".startsWith(UNI_ID_ORG_PREFIX), "uniprot")
          .otherwise("other")
      )
      // Assume last URL component contains identifiers
      // See https://github.com/opentargets/data_pipeline/blob/329ff219f9510d137c7609478b05d358c9195579/mrtarget/common/EvidenceString.py#L275
      .mutateColumns("target.id")(Functions.getUrlBasename)

    // Resolve UniProt target identifiers
    val dfru = dfr
      // Join all target ids to UniProt ids (most will not match)
      .join(dfu, dfr("target.id") === dfu("uniprot:uniprot_id"), "left")
      // Create field with original id to be mapped from as provenance
      .withColumn("target_id_uniprot",
        when(
          $"uniprot:ensembl_gene_id".isNotNull && ($"target_id_type" === "uniprot"),
          $"uniprot:uniprot_id"
        ).otherwise(null)
      )
      // Replace the target id with whatever value it matched to
      .mutateColumns("target.id")(c => {
        when($"target_id_uniprot".isNotNull, $"uniprot:ensembl_gene_id")
          .otherwise(c)
      })
      .pipe(df => df.drop(df.columns.filter(_.startsWith("uniprot:")):_*))

    // Resolve "non-reference" target identifiers
    val dff = dfru
      // Join to reference/alternate genes on the alternate id
      .join(dfn, dfru("target.id") === dfn("nonref:alternate"), "left")
      .withColumn("target_id_nonrefalt",
        when(
          $"nonref:reference".isNotNull && ($"target_id_type" =!= "other"),
          $"nonref:alternate"
        ).otherwise(null)
      )
      // Use the "reference" id for a matched alternate id if possible, otherwise leave the id alone
      .mutateColumns("target.id")(c => {
        when($"target_id_nonrefalt".isNotNull, $"nonref:reference")
          .otherwise(c)
      })
      .pipe(df => df.drop(df.columns.filter(_.startsWith("nonref:")):_*))

    dff
      // Pack the fields for transformation context into a separate struct
      .withStruct("context", "target_id_type", "target_id_uniprot", "target_id_nonrefalt")
  }

  def normalizeDiseaseIds(df: DataFrame): DataFrame = {
    // Replace raw EFO url strings with a single identifier
    df.mutateColumns("disease.id")(Functions.parseDiseaseIdFromUrl)
  }

  def validateTargetIds(df: DataFrame): DataFrame = {

    // TODO: Look for diseases and genes not indexes, log summaries of missing, remove bad records
    // TODO: check all these: https://github.com/opentargets/data_pipeline/blob/329ff219f9510d137c7609478b05d358c9195579/mrtarget/common/EvidenceString.py#L334
    df
  }

  def fixFields(df: DataFrame): DataFrame = {
    // TODO: catch everything in https://github.com/opentargets/data_pipeline/blob/329ff219f9510d137c7609478b05d358c9195579/mrtarget/common/EvidenceString.py#L153
    ???
  }

  override def spec(): Spec = {
    // TODO: Add id hash to unparseable/invalid records? (https://github.com/opentargets/data_pipeline/blob/329ff219f9510d137c7609478b05d358c9195579/mrtarget/modules/Evidences.py#L132)
    // TODO: Validate data source against datasource_to_datatypes? (https://github.com/opentargets/data_pipeline/blob/329ff219f9510d137c7609478b05d358c9195579/mrtarget/modules/Evidences.py#L158)
    Pipeline
      .Builder(config)
      .start("getEvidenceRawData", () => ss.read.textFile(config.rawEvidencePath))
      .andThen("runEvidenceSchemaValidation", runEvidenceSchemaValidation)
      .andThen("parseEvidenceData", parseEvidenceData)
      .andThen("normalizeTargetIds", normalizeTargetIds)
      .andThen("normalizeDiseaseIds", normalizeDiseaseIds)
      .andThen("validateTargetIds", validateTargetIds)
      .end("end")
  }
}
