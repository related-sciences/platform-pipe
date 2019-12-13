package com.relatedsciences.opentargets.etl.pipeline
import java.nio.file.Paths

import com.relatedsciences.opentargets.etl.configuration.Configuration.Config
import com.relatedsciences.opentargets.etl.pipeline.Pipeline.Spec
import com.relatedsciences.opentargets.etl.schema.Fields
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{array, struct, when}

class ScoringPreparationPipeline(ss: SparkSession, config: Config)
    extends SparkPipeline(ss, config)
    with LazyLogging {
  import ss.implicits._

  /**
    * Extract nested fields and subset raw evidence strings to tabular frame.
    */
  def getRawEvidence: DataFrame = {
    val resourceDataFields = Fields.allColumns.toList
    ss.read
      .json(config.evidenceExtractPath)
      .select(
        $"target.id".as("target_id"),
        $"private.efo_codes".as("efo_codes"),
        $"disease.id".as("disease_id"),
        $"type".as("type_id"),
        $"sourceID".as("source_id"),
        $"id",
        struct(resourceDataFields: _*).as("resource_data")
      )
  }

  /**
    * Prepare the list of disease ids (i.e. EFO codes) for expansion following individual
    * evidence string scoring.
    *
    * See: https://github.com/opentargets/data_pipeline/blob/e8372eac48b81a337049dd6b132dd69ff5cc7b64/mrtarget/modules/Association.py#L321
    */
  def prepareDiseaseIds(df: DataFrame): DataFrame = {
    // TODO: move to config
    val direct_data_sources = List("expression_atlas")
    df
    // Create disease ids to explode as 1-item array with terminal (i.e. lowest in EFO ontology)
    // disease id if source not configured for attribution to other diseases that are ancestors within EFO;
    // otherwise use efo_codes field which always contains the original disease id as well as all ancestors
      .withColumn("is_direct_source", $"source_id".isin(direct_data_sources: _*))
      .withColumn(
        "efo_ids",
        when($"is_direct_source", array($"disease_id"))
          .otherwise($"efo_codes")
      )
      // Rename the original disease_id field so it is clear that it is no longer the
      // constitutive identifier for the association
      .withColumnRenamed("disease_id", "terminal_disease_id")
  }

  override def spec(): Spec = {
    Pipeline
      .Builder(config)
      .start("getRawEvidence", getRawEvidence _)
      .andThen("prepareDiseaseIds", prepareDiseaseIds)
      .stop("savePreprocessed", save(_, config.preparedEvidencePath))
  }

}
