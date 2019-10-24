/**
 * OpenTargets scoring pipeline based on https://github.com/opentargets/data_pipeline
 *
 * Usage: /usr/spark-2.4.1/bin/spark-shell --driver-memory 4g --executor-memory 6g --executor-cores 8 -i ScoringPipeline.scala
 */
package com.relatedsciences.opentargets.pipeline
import com.relatedsciences.opentargets.pipeline.{Configuration => Config}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import java.text.SimpleDateFormat
import java.util.Calendar

class Pipeline(spark: SparkSession) {

  import spark.implicits._

  object Logger {
    def info(msg: String) = {
      val format = new SimpleDateFormat("y-M-d H:m:s")
      val timestamp = format.format(Calendar.getInstance().getTime())
      println(timestamp + ": " + msg)
    }
  }

  /**
   * Extract nested fields and subset raw evidence strings to tabular frame.
   */
  def getEvidenceDF(): DataFrame = {
    spark.read.json(Config.INPUT_DIR.resolve("evidence.json").toString())
      .select(
        $"target.id".as("target_id"),
        $"private.efo_codes".as("efo_codes"),
        $"disease.id".as("disease_id"),
        $"scores.association_score".as("score"),
        $"sourceID".as("source_id"),
        $"id"
      )
  }

  /**
   * Evidence strings are unique to a target but not to a disease so this transformation
   * will explode records based on all disease ids (expected to exist as arrays in a row).
   *
   * See: https://github.com/opentargets/data_pipeline/blob/7098546ee09ca1fc3c690a0bd6999b865ddfe646/mrtarget/modules/Association.py#L317
   */
  def explodeByDiseaseId(df: DataFrame): DataFrame = {
    val direct_data_sources = List("expression_atlas")
    df
      .select("id", "source_id", "disease_id", "target_id", "efo_codes", "score")
      // A "direct" source is one for which expansion to all EFO codes for a given disease is not allowed
      .withColumn("is_direct_source", $"source_id".isin(direct_data_sources:_*))
      // Create disease ids to explode as 1-item array, or as existing id array in "efo_codes"
      .withColumn(
        "efo_ids",
        when($"is_direct_source", array($"disease_id"))
          .otherwise($"efo_codes")
      )
      // Rename the original disease_id field so it is clear that it is no longer the primary identifier
      .withColumnRenamed("disease_id", "orig_disease_id")
      .select(
        $"id", $"source_id", $"orig_disease_id", $"target_id", $"score",
        explode($"efo_ids").as("disease_id")
      )
      // This flag is important for downstream filtering of evidence records based on
      // whether or not they relate to the primary disease or just an associated one
      .withColumn(
        "is_direct_id",
        $"orig_disease_id" === $"disease_id"
      )
  }

  /**
   * For each evidence record, determine the "harmonic score" as the rank of that score (descending) within a
   * single target, disease, and source, and then use that rank to set score = raw_score / (rank ** 2). This
   * score can then be summed to a particular level of aggregation (association-level or datatype-level).
   *
   * See: https://github.com/opentargets/data_pipeline/blob/7098546ee09ca1fc3c690a0bd6999b865ddfe646/mrtarget/common/Scoring.py#L66
   */
  def computeSourceScores(df: DataFrame): DataFrame = {
    val config = Configuration.loadScoringConfig(Config.CONFIG_DIR.resolve("scoring.yml").toString)
    val lkp = typedLit(config)
    df
      // Compute score for source using source-specific weights times the original evidence score
      .withColumnRenamed("score", "score_evidence")
      .withColumn("score_source", $"score_evidence" * coalesce(lkp($"source_id"), lit(1.0)))

      // Create harmonic score series for summation by ranking rows and using row number as denominator
      .withColumn("rid", row_number().over(
        Window
          .partitionBy("target_id", "disease_id", "source_id")
          .orderBy($"score_source".desc)
      ))
      .withColumn("score", $"score_source" / pow($"rid", 2.0))
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
    df
      .withColumn("rid", row_number().over(
        Window
          .partitionBy("target_id", "disease_id")
          .orderBy($"score".desc)
      ))
      .filter($"rid" <= 100)
      .withColumn("score", $"score" / pow($"rid", 2.0))
      .groupBy("target_id", "disease_id")
      .agg(
        sum("score").as("score"),
        max("is_direct").as("is_direct"),
        collect_set("source_id").as("source_ids")
      )
  }

  def execute(): Unit = {
    val logger = Logger

    logger.info("Beginning scoring pipeline")

    logger.info("Exploding evidence data and computing raw scores")
    val df = getEvidenceDF()
      .transform(explodeByDiseaseId)
      .transform(computeSourceScores)

    logger.info("Computing source level scores")
    val dfs = df.transform(aggregateSourceScores)

    logger.info("Computing association level scores")
    val dfa = dfs.transform(aggregateAssociationScores)

    var path = Config.OUTPUT_DIR.resolve("score_source.parquet")
    dfs.write.format("parquet").mode("overwrite").save(path.toString)
    logger.info(s"Saved source-level data to $path")

    path = Config.OUTPUT_DIR.resolve("score_association.parquet")
    dfa.write.format("parquet").mode("overwrite").save(path.toString)
    logger.info(s"Saved source-level data to $path")

    logger.info("Pipeline complete")
  }
}
