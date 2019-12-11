package com.relatedsciences.opentargets.etl.pipeline

import java.net.URL
import java.util.Optional

import com.relatedsciences.opentargets.etl.schema.Parser
import org.everit.json.schema.{FormatValidator, Schema, ValidationException, Validator}
import org.everit.json.schema.loader.SchemaLoader
import org.json.{JSONException, JSONObject, JSONTokener}

import scala.util.Try

object JsonValidation {

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
              res.copy(
                reason = Some("noncompliance_with_json_schema"),
                error = Some(e.toJSON.toString)
              )
          }
        }
      } catch {
        case e: JSONException =>
          ValidationResult(reason = Some("corrupt_json"), error = Some(e.getMessage))
      }
    }
  }

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
          Optional.of(
            s"[$value] is not a valid ${formatName()}. Expected one of: ${PARSER.formats}"
          )
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
        case true  => Optional.empty()
        case false => Optional.of(s"URI value cannot be empty")
      }
    }

    override def formatName(): String = "uri"

  }

}
