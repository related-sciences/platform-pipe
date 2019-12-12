package com.relatedsciences.opentargets.etl.pipeline

import java.net.URL

import com.relatedsciences.opentargets.etl.Common.{ENS_ID_ORG_PREFIX, UNI_ID_ORG_PREFIX}
import com.relatedsciences.opentargets.etl.Utilities
import com.relatedsciences.opentargets.etl.configuration.Configuration.Config
import com.relatedsciences.opentargets.etl.pipeline.JsonValidation.RecordValidator
import com.relatedsciences.opentargets.etl.pipeline.Pipeline.Spec
import com.relatedsciences.opentargets.etl.schema.{DataSource, DataType}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{when, _}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.io.Source

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
  import com.relatedsciences.opentargets.etl.pipeline.SparkImplicits._
  import ss.implicits._

  // Initialize static schema location from dynamic configuration --
  // is there a better way to do this that still results in a serializable Spark task?
  { RecordValidator.url = Some(new URL(config.dataResources.evidenceJsonSchema)) }

  lazy val validationResultSchema: StructType =
    ScalaReflection.schemaFor[ValidationResult].dataType.asInstanceOf[StructType]

  /**
    * Load gene index export (currently from ES)
    *
    * Approx. 60.5k rows
    */
  lazy val getGeneIndexData: DataFrame = {
    ss.read.json(config.geneDataPath)
  }

  /**
    * Load EFO index export (currently from ES)
    *
    * Approx. 15.6k rows
    */
  lazy val getEFOIndexData: DataFrame = {
    ss.read.json(config.efoDataPath)
  }

  /**
  * Load ECO score lookup mapping ECO URIs to static score values
    *
    * The file read should have a structure like this with no headers and fields [uri, code, score]:
    * http://identifiers.org/eco/cttv_mapping_pipeline	cttv_mapping_pipeline	1.
    * http://purl.obolibrary.org/obo/SO_0001621	NMD_transcript_variant	0.65
    * http://purl.obolibrary.org/obo/SO_0002165	trinucleotide_repeat_expansion	1.
    * ...
    *
    * At TOW, this mapping consists of 43 records.
    * @return map of ECO URIs to score values (code is ignored)
    */
  def getECOScores: Map[String, Double] = {
    Utilities.using(Source.fromURL(config.dataResources.ecoScores)) {
      source => source.mkString
        .split("\n")
        .map(_.split("\t"))
        .map(r => r(0) -> r(2).toDouble).toMap
    }
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
        when(trim($"uniprot_id") =!= "", $"uniprot_id")
        // otherwise null
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
      .cache()
  }

  def runEvidenceSchemaValidation(df: Dataset[String]): DataFrame = {
    val validatorUdf =
      udf((record: String) => RecordValidator.validate(record), validationResultSchema)
    df.withColumn("validation", validatorUdf($"value"))
      .select($"value", $"validation.*")
      .withColumnRenamed("isValid", "is_valid")
      // Save bad records as well as their frequencies by source and cause
      .transform(summarizeValidationErrors(config.evidenceSchemaValidationSummaryPath,
                                           config.evidenceSchemaValidationErrorsPath))
      .filter($"is_valid")
  }

  def parseEvidenceData(df: DataFrame): DataFrame = {
    ss.read
      .json(df.select("value").as[String])
      // Add source name to unique association fields as the identifier hash input
      // See: https://github.com/opentargets/data_pipeline/blob/329ff219f9510d137c7609478b05d358c9195579/mrtarget/modules/Evidences.py#L190
      .withColumn("id", md5(to_json(struct("unique_association_fields.*", "sourceID"))))
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
        ) // otherwise null
      )
      // Replace the target id with whatever value it matched to
      .mutateColumns("target.id")(c => {
        when($"target_id_uniprot".isNotNull, $"uniprot:ensembl_gene_id")
          .otherwise(c)
      })
      .transform(df => df.drop(df.columns.filter(_.startsWith("uniprot:")): _*))
      .transform(
        assertSizesEqual(dfr, "Num evidence rows changed after join (expected = %s, actual = %s)"))

    // Resolve "non-reference" target identifiers
    val dff = dfru
    // Join to reference/alternate genes on the alternate id
      .join(dfn, dfru("target.id") === dfn("nonref:alternate"), "left")
      .withColumn(
        "target_id_nonrefalt",
        when(
          $"nonref:reference".isNotNull && ($"target_id_type" =!= "other"),
          $"nonref:alternate"
        ) // otherwise null
      )
      // Use the "reference" id for a matched alternate id if possible, otherwise leave the id alone
      .mutateColumns("target.id")(c => {
        when($"target_id_nonrefalt".isNotNull, $"nonref:reference")
          .otherwise(c)
      })
      .transform(df => df.drop(df.columns.filter(_.startsWith("nonref:")): _*))
      .transform(
        assertSizesEqual(dfru, "Num evidence rows changed after join (expected = %s, actual = %s)"))

    dff
    // Pack the fields for transformation context into a separate struct
      .appendStruct("context", "target_id_type", "target_id_uniprot", "target_id_nonrefalt")
  }

  def normalizeDiseaseIds(df: DataFrame): DataFrame = {
    // Replace raw EFO url strings with a single identifier
    df.mutateColumns("disease.id")(Functions.parseDiseaseIdFromUrl)
  }

  /**
    * Clean up source/type names that are not mapped as desired upstream
    */
  def normalizeDataSources(df: DataFrame): DataFrame = {
    // Converts from eva + somatic_mutation -> eva_somatic no longer appears necessary since this
    // combination does not occur in the raw evidence now (must have been fixed upstream).  For posterity:
    // - https://github.com/opentargets/data_pipeline/blob/329ff219f9510d137c7609478b05d358c9195579/mrtarget/common/EvidenceString.py#L183
    // - df.withColumn("source_assignment", when($"sourceID" === DataSource.eva.toString && $"type" === DataType.somatic_mutation.toString, DataSource.eva_somatic.toString))
    //     .withColumn("sourceID", coalesce($"source_assignment", $"sourceID"))
    // "genetic_literature" types do occur regularly though, so include that normalization:
    df.withColumn("type_assignment",
                  when($"type" === DataType.genetic_literature.toString,
                       DataType.genetic_association.toString))
      .withColumn("type", coalesce($"type_assignment", $"type"))
      .appendStruct("context", "type_assignment")
  }

  /**
  * Enforce constraints on resource_score values based on fixed ECO code to score relationships
    *
    * Note that at TOW, the number of records updated by this routine is massive.  Of 1.7M evidence
    * records, almost 400k correspond to genetic_association records and ALL of them end up being assigned
    * new resource scores (about 100k b/c the original resource score is null and 300k because it is
    * provided as the "pvalue" type and not "probability" type)
    */
  def normalizeResourceScores(df: DataFrame): DataFrame = {
    // See https://github.com/opentargets/data_pipeline/blob/329ff219f9510d137c7609478b05d358c9195579/mrtarget/common/EvidenceString.py#L197
    val normalize = $"type" === DataType.genetic_association.toString && $"evidence.gene2variant".isNotNull
    val ecolkp = typedLit(getECOScores)
    // Override resources scores only for ECO codes that are in the static score mapping (i.e. "enforce" them
    // when known, otherwise leave them alone)
    df
      // Extract current resource score
      .withColumn("resource_score_original", when(normalize,
        coalesce($"evidence.gene2variant.resource_score.value", $"evidence.resource_score.value")
      ))
      // Get ECO URI for evidence
      .withColumn("resource_score_eco_uri", when(normalize,
        coalesce($"evidence.gene2variant.functional_consequence", $"evidence.evidence_codes".getItem(0))
      ))
      // Get expected score based on static URI -> score map
      .withColumn("resource_score_expected", when(normalize, ecolkp($"resource_score_eco_uri")))
      // Add flag for whether or not this "enforcement" is applicable
      .withColumn("resource_score_enforced_by_eco_code", $"resource_score_expected".isNotNull)
      // Add flag to indicate what the value was changed to, if any change occurs
      .withColumn("resource_score_enforced_update",
        when($"resource_score_enforced_by_eco_code",
            when($"resource_score_original".isNull, lit("value_null"))
            .when($"resource_score_original" =!= $"resource_score_expected", lit("value_unequal"))
            .otherwise(lit("value_equal"))
        )
      )
      // Leave the resource score as is unless an override was applicable
      .withColumn("evidence.gene2variant.resource_score",
        // This struct must match the field order of the original and specify all of them
        when($"resource_score_enforced_by_eco_code", struct(
          $"evidence.gene2variant.resource_score.method".as("method"),
          lit("probability").as("type"),
          $"resource_score_expected".as("value")
        ))
        .otherwise($"evidence.gene2variant.resource_score")
      )
      // Drop some intermediate fields, move others to retained context
      .drop("resource_score_original", "resource_score_eco_uri", "resource_score_expected")
      .appendStruct("context", "resource_score_enforced_by_eco_code", "resource_score_enforced_update")
  }

  def validateTargetIds(df: DataFrame): DataFrame = {
    val dfg = getGeneIndexData.select("id", "biotype").addPrefix("gene_index:")

    // Load map of data_source -> array[excluded_biotype]
    val btlkp = typedLit(config.pipeline.evidence.excludedBiotypes)

    // Join to gene index data to detect unmapped gene/target ids and apply biotype filter
    // TODO: double check these: https://github.com/opentargets/data_pipeline/blob/329ff219f9510d137c7609478b05d358c9195579/mrtarget/common/EvidenceString.py#L334
    df.join(dfg, df("target.id") === dfg("gene_index:id"), "left")
      .withColumn("excluded_biotypes", btlkp($"sourceID"))
      .withColumn(
        "reason",
        when($"target.id".isNull, "id_null")
          .when($"gene_index:id".isNull, "id_not_found")
          .when(array_contains($"excluded_biotypes", $"gene_index:biotype"), "biotype_exclusion")
        // otherwise null
      )
      .withColumn("is_valid", $"reason".isNull)
      .transform(
        assertSizesEqual(df, "Num evidence rows changed after join (expected = %s, actual = %s)"))
      .transform(summarizeValidationErrors(config.evidenceTargetIdValidationSummaryPath,
                                           config.evidenceTargetIdValidationErrorsPath))
      .filter($"is_valid")
      .drop("is_valid", "reason", "excluded_biotypes", "gene_index:id", "gene_index:biotype")
      .transform(assertSchemasEqual(df))
  }

  def validateDiseaseIds(df: DataFrame): DataFrame = {
    val dfd = getEFOIndexData.select($"id".as("efo_index:id"))

    // Join to gene index data to detect unmapped gene/target ids
    df.join(dfd, df("disease.id") === dfd("efo_index:id"), "left")
      .withColumn(
        "reason",
        when($"disease.id".isNull, "id_null")
          .when($"efo_index:id".isNull, "id_not_found")
          .otherwise(null)
      )
      .withColumn("is_valid", $"reason".isNull)
      .transform(
        assertSizesEqual(df, "Num evidence rows changed after join (expected = %s, actual = %s)"))
      .transform(summarizeValidationErrors(config.evidenceDiseaseIdValidationSummaryPath,
                                           config.evidenceDiseaseIdValidationErrorsPath))
      .filter($"is_valid")
      .drop("reason", "is_valid", "efo_index:id")
      .transform(assertSchemasEqual(df))
  }

  def validateDataSources(df: DataFrame): DataFrame = {
    val validDataSources = config.dataSources.map(_.name)
    df.withColumn("reason",
                  when(!$"sourceID".isin(validDataSources: _*), "invalid_data_source")
                  // otherwise null
      )
      .withColumn("is_valid", $"reason".isNull)
      .transform(summarizeValidationErrors(config.evidenceDataSourceValidationSummaryPath,
                                           config.evidenceDataSourceValidationErrorsPath))
      .filter($"is_valid")
      .drop("is_valid", "reason")
      // Assert schema b/c all "validate*" methods should only filter, not mutate
      .transform(assertSchemasEqual(df))
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
    Pipeline
      .Builder(config)
      .start("getEvidenceRawData", () => ss.read.textFile(config.rawEvidencePath))
      .andThen("runEvidenceSchemaValidation", runEvidenceSchemaValidation)
      .andThen("parseEvidenceData", parseEvidenceData)
      .andThen("normalizeTargetIds", normalizeTargetIds)
      .andThen("normalizeDiseaseIds", normalizeDiseaseIds)
      .andThen("normalizeDataSources", normalizeDataSources)
      .andThen("normalizeResourceScores", normalizeResourceScores)
      .andThen("validateTargetIds", validateTargetIds)
      .andThen("validateDiseaseIds", validateDiseaseIds)
      .andThen("validateDataSources", validateDataSources)
      .end("end")
  }
}
