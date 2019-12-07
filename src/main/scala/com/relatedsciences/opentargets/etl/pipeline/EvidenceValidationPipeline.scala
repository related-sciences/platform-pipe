package com.relatedsciences.opentargets.etl.pipeline

import java.net.URL
import java.time.format.{DateTimeFormatterBuilder, DateTimeParseException}
import java.util.Optional

import com.relatedsciences.opentargets.etl.configuration.Configuration.Config
import com.relatedsciences.opentargets.etl.pipeline.Components.Spec
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.everit.json.schema.{FormatValidator, Schema, ValidationException, Validator}
import org.everit.json.schema.loader.SchemaLoader
import org.json.{JSONException, JSONObject, JSONTokener}

class EvidenceValidationPipeline(ss: SparkSession, config: Config)
    extends SparkPipeline(ss, config)
    with LazyLogging {

  case class ValidationResult(isValid: Boolean, reason: Option[String], error: Option[String])
  lazy val validationResultSchema: StructType =
    ScalaReflection.schemaFor[ValidationResult].dataType.asInstanceOf[StructType]

  class DateValidator extends FormatValidator {

    val FORMAT = "yyyy-MM-dd'T'HH:mm:ss"
    val FORMATTER = new DateTimeFormatterBuilder()
      .appendPattern(FORMAT)
      .toFormatter();

    override def validate(subject: String): Optional[String] = {
      try {
        FORMATTER.parse(subject)
        Optional.empty()
      } catch {
        case e: DateTimeParseException =>
          Optional.of(s"[$subject] is not a valid ${formatName()}. Expected $FORMAT")
      }
    }

    override def formatName(): String = "date-time"
  }

  object RecordValidator {

    @transient lazy val schema: Schema = {
      val url = new URL(
        "https://raw.githubusercontent.com/opentargets/json_schema/master/opentargets.json"
      )
      SchemaLoader
        .builder()
        .schemaJson(new JSONObject(new JSONTokener(url.openStream())))
        .enableOverrideOfBuiltInFormatValidators()
        .addFormatValidator(new DateValidator())
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
        this.validator.performValidation(this.schema, new JSONObject(rec))
        ValidationResult(true, None, None)
      } catch {
        case e: JSONException => ValidationResult(false, Some("corrupt"), Some(e.getMessage))
        case e: ValidationException =>
          ValidationResult(false, Some("invalid"), Some(e.toJSON.toString))
      }
    }
  }

  def validate(): DataFrame = {
    val validatorUdf =
      udf((record: String) => RecordValidator.validate(record), validationResultSchema)
    val df = ss.read
      .textFile(config.rawEvidencePath)
      .withColumn("validation", validatorUdf(col("value")))
      .select(col("value"), col("validation.*"))
    // TODO: Save summary of validation errors, remove invalid records, expand to dataset, save
    // val ds = ss.read.json(df.select("value").rdd.map(r => r.getAs[String](0)))
    df
  }

  override def spec(): Spec = {
    Pipeline
      .Builder(config)
      .start("validate", validate _)
      .end("end")
  }
}
