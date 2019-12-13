package com.relatedsciences.opentargets.etl.pipeline

import com.relatedsciences.opentargets.etl.Record
import com.relatedsciences.opentargets.etl.configuration.Configuration.Config
import com.relatedsciences.opentargets.etl.pipeline.Pipeline.Spec
import com.relatedsciences.opentargets.etl.scoring.{Parameters, Score, Scoring}
import com.relatedsciences.opentargets.etl.scoring.Scoring.UnsupportedDataTypeException
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, collect_set, explode, lit, max, pow, row_number, sum, typedLit, udf, when}

class ScoringCalculationPipeline(ss: SparkSession, config: Config)
    extends SparkPipeline(ss, config)
    with LazyLogging {
  import ss.implicits._

  /**
    * Determine scores for individual evidence strings prior to expansion by EFO code.
    *
    * This method computes the "scores.association_score" field added prior to all other scoring.
    * See: https://github.com/opentargets/data_pipeline/blob/7098546ee09ca1fc3c690a0bd6999b865ddfe646/mrtarget/common/EvidenceString.py#L570
    */
  def getResourceScores: DataFrame = {
    val allowUnknownDataType = config.pipeline.scoring.allowUnknownDataType
    val scoringUdf = udf(
      (id: String, typeId: String, sourceId: String, resourceData: Row) =>
        try {
          val record = new Record(id, typeId, sourceId, resourceData)
          val params = Parameters.default()
          Scoring.score(record, params).get
        } catch {
          case _: UnsupportedDataTypeException if allowUnknownDataType => 0.0
          case e: Throwable                                            => throw e
        },
      Score.Schema
    )
    ss.read
      .parquet(config.preparedEvidencePath)
      .withColumn(
        "score_resource",
        scoringUdf($"id", $"type_id", $"source_id", $"resource_data")("score")
      )
  }

  /**
    * Evidence strings are unique to a target but not to a disease so this transformation
    * will explode records based on all disease ids (expected to exist as arrays in a row).
    *
    * See: https://github.com/opentargets/data_pipeline/blob/7098546ee09ca1fc3c690a0bd6999b865ddfe646/mrtarget/modules/Association.py#L317
    */
  def explodeByDiseaseId(df: DataFrame): DataFrame = {
    df
    // Explode disease ids into new rows
      .select(
        $"id",
        $"source_id",
        $"terminal_disease_id",
        $"target_id",
        $"score_resource",
        explode($"efo_ids").as("disease_id")
      )
      .withColumn("is_direct_id", $"terminal_disease_id" === $"disease_id")
  }

  /**
    * For each evidence record, determine the "harmonic score" as the rank of that score (descending) within a
    * single target, disease, and source, and then use that rank to set score = raw_score / (rank ** 2). This
    * score can then be summed to a particular level of aggregation (association-level or datatype-level).
    *
    * See: https://github.com/opentargets/data_pipeline/blob/7098546ee09ca1fc3c690a0bd6999b865ddfe646/mrtarget/common/Scoring.py#L66
    */
  def computeSourceScores(df: DataFrame): DataFrame = {
    val lkp = typedLit(config.pipeline.scoring.sourceWeights)
    df
    // Compute score for source using source-specific weights times the original evidence score
      .withColumn("score_source", $"score_resource" * coalesce(lkp($"source_id"), lit(1.0)))
      // Create harmonic score series for summation by ranking rows and using row number as denominator
      .withColumn(
        "rid",
        row_number().over(
          Window
            .partitionBy("target_id", "disease_id", "source_id")
            .orderBy($"score_source".desc)
        )
      )
      .withColumn("score", $"score_source" / pow($"rid", 2.0))
  }

  /**
    * Optionally, save the evidence scores prior to aggregation at any level.  This can be helpful for determining
    * what the constituents of any one score are but is generally not necessary in production runs.
    */
  def saveEvidenceScores(df: DataFrame): DataFrame = {
    if (config.pipeline.scoring.saveEvidenceScores) {
      val path = config.evidenceScorePath
      df.write.format("parquet").mode("overwrite").save(path.toString)
      logger.info(s"Saved evidence-level scores to $path")
    }
    df
  }

  /**
    * Aggregate harmonic scores to specific target, disease, and source combinations
    * (i.e. do the "source-level" harmonic sum)
    *
    * See: https://github.com/opentargets/data_pipeline/blob/7098546ee09ca1fc3c690a0bd6999b865ddfe646/mrtarget/modules/Association.py#L276
    */
  def aggregateSourceScores(df: DataFrame): DataFrame = {
    df
    // Constituents for any one score are limited to the 100 largest
    // (See: https://github.com/opentargets/data_pipeline/blob/e8372eac48b81a337049dd6b132dd69ff5cc7b64/mrtarget/modules/Association.py#L268)
      .filter($"rid" <= 100)
      .groupBy("target_id", "disease_id", "source_id")
      .agg(sum("score").as("score_raw"), max("is_direct_id").as("is_direct"))
      .withColumn("score", when($"score_raw" > 1, 1).otherwise($"score_raw"))
  }

  /**
    * Aggregate harmonic scores to specific target and disease combinations
    * (i.e. do the "association-level" harmonic sum)
    *
    * See: https://github.com/opentargets/data_pipeline/blob/7098546ee09ca1fc3c690a0bd6999b865ddfe646/mrtarget/modules/Association.py#L297
    */
  def aggregateAssociationScores(df: DataFrame): DataFrame = {
    df.withColumn(
        "rid",
        row_number().over(
          Window
            .partitionBy("target_id", "disease_id")
            .orderBy($"score".desc)
        )
      )
      .filter($"rid" <= 100)
      .withColumn("score", $"score" / pow($"rid", 2.0))
      .groupBy("target_id", "disease_id")
      .agg(
        sum("score").as("score"),
        max("is_direct").as("is_direct"),
        collect_set("source_id").as("source_ids")
      )
  }

  override def spec(): Spec = {
    Pipeline
      .Builder(config)
      .start("getResourceScores", getResourceScores _)
      .andThen("explodeByDiseaseId", explodeByDiseaseId)
      .andThen("computeSourceScores", computeSourceScores)
      .andThen("saveEvidenceScores", saveEvidenceScores)
      .andThen("aggregateSourceScores", aggregateSourceScores)
      .andThen("saveSourceScores", save(_, config.sourceScorePath))
      .andThen("aggregateAssociationScores", aggregateAssociationScores)
      .stop("saveAssociationScores", save(_, config.associationScorePath))
  }
}
