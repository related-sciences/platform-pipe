package com.relatedsciences.opentargets.etl.pipeline

import java.net.URL

import com.relatedsciences.opentargets.etl.configuration.Configuration.Config
import com.relatedsciences.opentargets.etl.pipeline.JsonValidation.RecordValidator
import com.relatedsciences.opentargets.etl.pipeline.Pipeline.Spec
import com.relatedsciences.opentargets.etl.Common.{ENS_ID_ORG_PREFIX, UNI_ID_ORG_PREFIX}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{when, _}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

case class ValidationResult(
    isValid: Boolean = false,
    reason: Option[String] = None,
    error: Option[String] = None,
    sourceId: Option[String] = None,
    dataType: Option[String] = None,
    targetId: Option[String] = None,
    diseaseId: Option[String] = None
)

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


  private def summarizeErrors(df: DataFrame, summaryPath: String, recordsPath: String): DataFrame = {
    save(df.groupBy("sourceID", "reason").count(), summaryPath)
    save(df.filter(!$"is_valid"), recordsPath)
    df
  }

  def runEvidenceSchemaValidation(df: Dataset[String]): DataFrame = {
    val validatorUdf =
      udf((record: String) => RecordValidator.validate(record), validationResultSchema)
    val dfv = df
      .withColumn("validation", validatorUdf($"value"))
      .select($"value", $"validation.*")
      .withColumnRenamed("isValid", "is_valid")

    // Save bad records as well as the frequencies by source and cause
    summarizeErrors(dfv, config.evidenceSchemaValidationSummaryPath, config.evidenceSchemaValidationErrorsPath)
    dfv.filter($"is_valid")
  }

  def parseEvidenceData(df: DataFrame): DataFrame = {
    ss.read
      .json(df.select("value").as[String])
      // Add source name to unique association fields as the identifier hash input
      // See: https://github.com/opentargets/data_pipeline/blob/329ff219f9510d137c7609478b05d358c9195579/mrtarget/modules/Evidences.py#L190
      .withColumn("id", md5(to_json(struct("unique_association_fields.*", "sourceID"))))
  }

  /**
    * Load gene index export (currently from ES)
    *
    * Approx. 60.5k rows
    */
  def getGeneIndexData: DataFrame = {
    ss.read.json(config.geneDataPath)
  }

  /**
    * Load EFO index export (currently from ES)
    *
    * Approx. 15.6k rows
    */
  def getEFOIndexData: DataFrame = {
    ss.read.json(config.efoDataPath)
  }

  /**
    * Load Ensembl and UniProt ids such that uniprot ids include all accessions, with ensembl id repeated for each.
    * UniProt ids will be null when not applicable.
    *
    * Approx. 129k rows
    */
  def getUniProtGeneLookup: Dataset[UniProtGeneLookup] = {
    getGeneIndexData
      .select(
        $"ensembl_gene_id",
        explode_outer(
          concat(array($"uniprot_id"), $"uniprot_accessions")
        ).as("uniprot_id")
      )
      // Convert empty to null to avoid ambiguity post-downstream-join
      .withColumn(
        "uniprot_id",
        when(trim($"uniprot_id") =!= "", $"uniprot_id").otherwise(lit(null: String))
      )
      .as[UniProtGeneLookup]
  }

  /**
    * Load mapping of "non-reference" genes to genes with an associated reference genome (others correspond to
    * assemblies in highly polymorphic regions, e.g. HLA)
    *
    * Approx. 300 rows
    */
  def getNonReferenceGeneLookup: Dataset[NonReferenceGeneLookup] = {
    ss.read
      .option("header", "true")
      .csv(config.nonRefGeneDataPath)
      .as[NonReferenceGeneLookup]
  }

  def normalizeTargetIds(df: DataFrame): DataFrame = {
    // For each dataset to join on, make sure to drop duplicates (arbitrarily)
    // by the key to be joined ON (not the id field used to reassign target ids);
    // this is necessary to keep result from gaining new records
    val dfu = getUniProtGeneLookup
      .toDF()
      .addPrefix("uniprot:")
      .dropDuplicates("uniprot:uniprot_id")
    val dfn = getNonReferenceGeneLookup
      .toDF()
      .addPrefix("nonref:")
      .cache()
      .dropDuplicates("nonref:alternate")

    val dfr = df
    // Determine which type of identifier is used based on prefix
      .withColumn(
        "target_id_type",
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
      .withColumn(
        "target_id_uniprot",
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
      .pipe(df => df.drop(df.columns.filter(_.startsWith("uniprot:")): _*))
    assertSizesEqual(dfr, dfru, "Num evidence rows changed after join (expected = %s, actual = %s)")

    // Resolve "non-reference" target identifiers
    val dff = dfru
    // Join to reference/alternate genes on the alternate id
      .join(dfn, dfru("target.id") === dfn("nonref:alternate"), "left")
      .withColumn(
        "target_id_nonrefalt",
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
      .pipe(df => df.drop(df.columns.filter(_.startsWith("nonref:")): _*))
    assertSizesEqual(dfru, dff, "Num evidence rows changed after join (expected = %s, actual = %s)")

    dff
    // Pack the fields for transformation context into a separate struct
      .withStruct("context", "target_id_type", "target_id_uniprot", "target_id_nonrefalt")
  }

  def normalizeDiseaseIds(df: DataFrame): DataFrame = {
    // Replace raw EFO url strings with a single identifier
    df.mutateColumns("disease.id")(Functions.parseDiseaseIdFromUrl)
  }

  def validateTargetIds(df: DataFrame): DataFrame = {
    val dfg = getGeneIndexData.select("id", "biotype").addPrefix("gene_index:")

    // Join to gene index data to detect unmapped gene/target ids
    // TODO: biotype filter: https://github.com/opentargets/data_pipeline/blob/329ff219f9510d137c7609478b05d358c9195579/mrtarget/common/EvidenceString.py#L297
    // TODO: double check these: https://github.com/opentargets/data_pipeline/blob/329ff219f9510d137c7609478b05d358c9195579/mrtarget/common/EvidenceString.py#L334
    val btlkp = typedLit(config.pipeline.evidence.excludedBiotypes)
    val dfv = df
      .join(dfg, df("target.id") === dfg("gene_index:id"), "left")
      .withColumn("excluded_biotypes", btlkp($"sourceID"))
      .withColumn(
        "reason",
          when($"target.id".isNull, "id_null")
            .when($"gene_index:id".isNull, "id_not_found")
            .when(array_contains($"excluded_biotypes", $"gene_index:biotype"), "biotype_exclusion")
            .otherwise(null)
      )
      .withColumn("is_valid", $"reason".isNull)
    assertSizesEqual(df, dfv, "Num evidence rows changed after join (expected = %s, actual = %s)")
    summarizeErrors(dfv, config.evidenceTargetIdValidationSummaryPath, config.evidenceTargetIdValidationErrorsPath)

    // Return only valid records and drop added fields
    val dfr = dfv
      .filter($"is_valid")
      .drop("reason", "excluded_biotypes", "is_valid", "gene_index:id", "gene_index:biotype")
    assertSchemasEqual(df, dfr)
    dfr
  }

  def validateDiseaseIds(df: DataFrame): DataFrame = {
    val dfd = getEFOIndexData.select($"id".as("efo_index:id"))

    // Join to gene index data to detect unmapped gene/target ids
    val dfv = df
      .join(dfd, df("disease.id") === dfd("efo_index:id"), "left")
      .withColumn(
        "reason",
        when($"disease.id".isNull, "id_null")
          .when($"efo_index:id".isNull, "id_not_found")
          .otherwise(null)
      )
      .withColumn("is_valid", $"reason".isNull)
    assertSizesEqual(df, dfv, "Num evidence rows changed after join (expected = %s, actual = %s)")
    summarizeErrors(dfv, config.evidenceDiseaseIdValidationSummaryPath, config.evidenceDiseaseIdValidationErrorsPath)

    // Return only valid records and drop added fields
    val dfr = dfv
      .filter($"is_valid")
      .drop("reason", "is_valid", "efo_index:id")
    assertSchemasEqual(df, dfr)
    dfr
  }

  def fixFields(df: DataFrame): DataFrame = {
    // TODO: catch everything in https://github.com/opentargets/data_pipeline/blob/329ff219f9510d137c7609478b05d358c9195579/mrtarget/common/EvidenceString.py#L153
    ???
    //    dfv
    //      // Merge the new context into context struct
    //      .withStruct("context_new", "target_id_invalid_reason")
    //      .withColumn("context", struct($"context.*", $"context_new.*"))
    //      .drop("context_new")
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
      .andThen("validateDiseaseIds", validateDiseaseIds)
      .end("end")
  }
}
