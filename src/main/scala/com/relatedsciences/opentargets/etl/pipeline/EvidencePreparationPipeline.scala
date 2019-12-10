package com.relatedsciences.opentargets.etl.pipeline

import java.net.URL
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.util.Optional

import com.relatedsciences.opentargets.etl.configuration.Configuration.Config
import com.relatedsciences.opentargets.etl.pipeline.Pipeline.Spec
import com.relatedsciences.opentargets.etl.schema.Parser
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession, Dataset}
import org.everit.json.schema.loader.SchemaLoader
import org.everit.json.schema.{FormatValidator, Schema, ValidationException, Validator}
import org.json.{JSONException, JSONObject, JSONTokener}

import scala.util.Try

case class ValidationResult(isValid: Boolean = false,
                            reason: Option[String] = None,
                            error: Option[String] = None,
                            sourceId: Option[String] = None,
                            dataType: Option[String] = None,
                            targetId: Option[String] = None,
                            diseaseId: Option[String] = None)

/**
* Custom validator implementation based on:
  * https://github.com/everit-org/json-schema/blob/master/core/src/main/java/org/everit/json/schema/internal/DateTimeFormatValidator.java
  *
  * This is necessary to accommodate dates found in evidence data not matching default everit-org date patterns
  */
class DateValidator extends FormatValidator {

  val PARSER = new Parser.DateParser()

  override def validate(value: String): Optional[String] = {
    // org.everit.json.schema.FormatValidator requires None for valid entries and Some(reason) otherwise
    PARSER.parse(value) match {
      case Some(_) => Optional.empty()
      case None =>
        Optional.of(s"[$value] is not a valid ${formatName()}. Expected one of: ${PARSER.formats}")
    }
  }

  override def formatName(): String = "date-time"
}

/**
  * Custom validator implementation based on:
  * https://github.com/everit-org/json-schema/blob/master/core/src/main/java/org/everit/json/schema/internal/URIFormatValidator.java
  */
class URIFormatValidator extends FormatValidator {

  override def validate(value: String): Optional[String] = {
    value.trim.nonEmpty match {
      case true => Optional.empty()
      case false => Optional.of(s"URI value cannot be empty")
    }
  }

  override def formatName(): String = "uri"

}

case class UniProtGeneLookup(ensembl_gene_id: String, uniprot_id: String)
case class NonReferenceGeneLookup(reference: String, alternate: String)

object RecordValidator {
  var url = Option.empty[URL]

  @transient lazy val schema: Schema = {
    val u = url match {
      case Some(url) => url
      case None      => throw new IllegalStateException("OpenTargets schema URL not set")
    }
    SchemaLoader
      .builder()
      .schemaJson(new JSONObject(new JSONTokener(u.openStream())))
      .enableOverrideOfBuiltInFormatValidators()
      .addFormatValidator(new DateValidator())
      .addFormatValidator(new URIFormatValidator())
      .build()
      .load()
      .build()
      .asInstanceOf[Schema]
  }

  @transient lazy val validator = Validator
    .builder()
    .failEarly()
    .build()

  def validate(rec: String): ValidationResult = {
    try {
      val obj = new JSONObject(rec)
      val res = ValidationResult().copy(
        sourceId = Try(Some(obj.getString("sourceID"))).getOrElse(Option.empty[String]),
        // TODO: check to see how often "label" is used instead (should come up in invalid results)
        dataType = Try(Some(obj.getString("type"))).getOrElse(Option.empty[String]),
        targetId =
          Try(Some(obj.getJSONObject("target").getString("id"))).getOrElse(Option.empty[String]),
        diseaseId =
          Try(Some(obj.getJSONObject("disease").getString("id"))).getOrElse(Option.empty[String])
      )
      if (res.sourceId.isEmpty) res.copy(reason = Some("missing_data_source"))
      else if (res.dataType.isEmpty) res.copy(reason = Some("missing_data_type"))
      else if (res.targetId.isEmpty) res.copy(reason = Some("missing_target_id"))
      else if (res.diseaseId.isEmpty) res.copy(reason = Some("missing_disease_id"))
      else {
        try {
          this.validator.performValidation(this.schema, obj)
          res.copy(isValid = true)
        } catch {
          case e: ValidationException =>
            res.copy(reason = Some("noncompliance_with_json_schema"), error = Some(e.toJSON.toString))
        }
      }
    } catch {
      case e: JSONException =>
        ValidationResult(reason = Some("corrupt_json"), error = Some(e.getMessage))
    }
  }
}

class EvidencePreparationPipeline(ss: SparkSession, config: Config)
    extends SparkPipeline(ss, config)
    with LazyLogging {
  import ss.implicits._
  import com.relatedsciences.opentargets.etl.pipeline.SparkImplicits._

  val UNI_ID_ORG_PREFIX = "http://identifiers.org/uniprot/"
  val ENS_ID_ORG_PREFIX = "http://identifiers.org/ensembl/"

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
      .withStructColumn("target", "id", element_at(split($"target.id", "/"), -1))
      // TODO: This should be split out like get_ontology_code_from_url (https://github.com/opentargets/data_pipeline/blob/329ff219f9510d137c7609478b05d358c9195579/mrtarget/modules/Evidences.py#L201)
      // TODO: Decide how to handle this check: https://github.com/opentargets/data_pipeline/blob/329ff219f9510d137c7609478b05d358c9195579/mrtarget/common/EvidenceString.py#L326
      .withStructColumn("disease", "id", element_at(split($"disease.id", "/"), -1))

    // Resolve UniProt target identifiers
    val dfru = dfr
      // Join all target ids to UniProt ids (most will not match)
      .join(dfu, dfr("target.id") === dfu("uniprot:uniprot_id"), "left")
      // Replace the target id with whatever value it matched to for UniProt ids only (it may be null)
      .withStructColumn(
        "target", "id",
        when($"target_id_type" === "uniprot", $"uniprot:ensembl_gene_id")
          .otherwise($"target.id")
      )
      .pipe(df => df.drop(df.columns.filter(_.startsWith("uniprot:")):_*))

    // Resolve "non-reference" target identifiers
    val dff = dfru
      // Join to reference/alternate genes on the alternate id
      .join(dfn, dfru("target.id") === dfn("nonref:alternate"), "left")
      // Use the "reference" id for a matched alternate id if possible, otherwise leave the id alone
      .withStructColumn(
        "target", "id",
        when($"nonref:reference".isNotNull, $"nonref:reference")
          .otherwise($"target.id")
      )
      .pipe(df => df.drop(df.columns.filter(_.startsWith("nonref:")):_*))

    dff
  }

  def validateTargetIds(df: DataFrame): DataFrame = {
    // TODO: Look for diseases and genes not indexes, log summaries of missing, remove bad records
    // TODO: check all these: https://github.com/opentargets/data_pipeline/blob/329ff219f9510d137c7609478b05d358c9195579/mrtarget/common/EvidenceString.py#L334
    df
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
      .andThen("validateTargetIds", validateTargetIds)
      .end("end")
  }
}
