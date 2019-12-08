package com.relatedsciences.opentargets.etl.pipeline

import java.net.URL
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.util.Optional

import com.relatedsciences.opentargets.etl.configuration.Configuration.Config
import com.relatedsciences.opentargets.etl.pipeline.Pipeline.Spec
import com.relatedsciences.opentargets.etl.schema.Parser
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{col, udf}
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
                            typeName: Option[String] = None,
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
        typeName = Try(Some(obj.getString("type"))).getOrElse(Option.empty[String]),
        targetId =
          Try(Some(obj.getJSONObject("target").getString("id"))).getOrElse(Option.empty[String]),
        diseaseId =
          Try(Some(obj.getJSONObject("disease").getString("id"))).getOrElse(Option.empty[String])
      )
      try {
        this.validator.performValidation(this.schema, obj)
        res.copy(isValid = true)
      } catch {
        case e: ValidationException =>
          res.copy(reason = Some("invalid"), error = Some(e.toJSON.toString))
      }
    } catch {
      case e: JSONException =>
        ValidationResult(reason = Some("corrupt"), error = Some(e.getMessage))
    }
  }
}

class EvidenceValidationPipeline(ss: SparkSession, config: Config)
    extends SparkPipeline(ss, config)
    with LazyLogging {
  import ss.implicits._

  // Initialize static schema location from dynamic configuration --
  // is there a better way to do this that still results in a serializable Spark task?
  { RecordValidator.url = Some(new URL(config.evidenceJsonSchema)) }

  lazy val validationResultSchema: StructType =
    ScalaReflection.schemaFor[ValidationResult].dataType.asInstanceOf[StructType]

  def getRawEvidenceData(): DataFrame = {
    val validatorUdf =
      udf((record: String) => RecordValidator.validate(record), validationResultSchema)
    ss.read
      .textFile(config.rawEvidencePath)
      .withColumn("validation", validatorUdf(col("value")))
      .select(col("value"), col("validation.*"))
  }

  def saveValidationSummary(df: DataFrame): DataFrame = {
    // Save a count of the validation statuses for each source
    this.save(df.groupBy("sourceID", "reason").count(), config.evidenceValidationSummaryPath)
    df
  }

  def parseEvidenceData(df: DataFrame): DataFrame = {
    val dfv = df.filter(col("isValid"))
    ss.read.json(dfv.select("value").as[String])
  }

  override def spec(): Spec = {
    Pipeline
      .Builder(config)
      .start("getRawEvidenceData", getRawEvidenceData _)
      .andThen("saveValidationSummary", saveValidationSummary)
      .andThen("parseEvidenceData", parseEvidenceData)
      .end("end")
  }
}
