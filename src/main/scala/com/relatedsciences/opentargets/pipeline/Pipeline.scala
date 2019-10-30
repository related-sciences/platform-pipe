/**
  * OpenTargets scoring pipeline based on https://github.com/opentargets/data_pipeline
  *
  * Usage (Interactive):
  * ```
  * /usr/spark-2.4.1/bin/spark-shell --driver-memory 12g --jars target/scala-2.11/ot-scoring_2.11-0.1.jar
  * > import com.relatedsciences.opentargets.pipeline.Pipeline
  * > new Pipeline(spark).runScoring(targets=Some(Seq("ENSG00000162434")), diseases=Some(Seq("EFO_0000181")))
  * ```
  */
package com.relatedsciences.opentargets.pipeline
import java.text.SimpleDateFormat
import java.util.Calendar

import com.relatedsciences.opentargets.pipeline.scoring.Scoring.UnsupportedDataTypeException
import com.relatedsciences.opentargets.pipeline.schema.Fields.{FieldName, FieldPath}
import com.relatedsciences.opentargets.pipeline.scoring.{Parameters, Score, Scoring}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class Pipeline(spark: SparkSession, config: Configuration = Configuration.default()) {

  import spark.implicits._

  /** TODO: Fix lazy ass logging; figure out how to choose framework compatible with other classpath entries */
  object Logger {
    def info(msg: String): Unit = {
      val format    = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val timestamp = format.format(Calendar.getInstance().getTime)
      println(timestamp + ": " + msg)
    }
  }

  /**
    * Extract nested fields and subset raw evidence strings to tabular frame.
    */
  def getRawEvidence: DataFrame = {
    val valueFields =
      (FieldName.values.map(FieldName.pathName), FieldName.values.map(FieldName.flatName))
    val pathFields =
      (FieldPath.values.map(FieldPath.pathName), FieldPath.values.map(FieldPath.flatName))
    val valueCols = valueFields.zipped.toList.sorted.map(t => col(t._1).as(t._2))
    val pathCols  = pathFields.zipped.toList.sorted.map(t => col(t._1).isNotNull.as(t._2))
    spark.read
      .json(config.inputPath.resolve("evidence.json").toString)
      .select(
        $"target.id".as("target_id"),
        $"private.efo_codes".as("efo_codes"),
        $"disease.id".as("disease_id"),
        $"type".as("type_id"),
        $"sourceID".as("source_id"),
        $"id",
        struct(valueCols ++ pathCols: _*).as("resource_data")
      )
  }

  /**
    * Prepare the list of disease ids (i.e. EFO codes) for expansion following individual
    * evidence string scoring.
    *
    * See: https://github.com/opentargets/data_pipeline/blob/e8372eac48b81a337049dd6b132dd69ff5cc7b64/mrtarget/modules/Association.py#L321
    */
  def prepareDiseaseIds(df: DataFrame): DataFrame = {
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

  /**
    * Load preprocessed frame
    */
  def getPreprocessedEvidence: DataFrame = {
    spark.read.parquet(config.outputPath.resolve("evidence.parquet").toString)
  }

  /**
    * Determine scores for individual evidence strings prior to expansion by EFO code.
    *
    * This method computes the "scores.association_score" field added prior to all other scoring.
    * See: https://github.com/opentargets/data_pipeline/blob/7098546ee09ca1fc3c690a0bd6999b865ddfe646/mrtarget/common/EvidenceString.py#L570
    */
  def computeResourceScores(df: DataFrame): DataFrame = {
    /* WARNING: keep the config out of the UDF closure as it will cause serialization errors */
    // TODO: Stop using path objects in the config -- that's probably why they won't serialize
    val allowUnknownDataType = config.allowUnknownDataType
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
    df.withColumn(
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
    val lkp = typedLit(config.getScoringConfig())
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

  def runPreprocessing(): Pipeline = {
    val logger = Logger
    logger.info("Beginning preprocessing pipeline")

    logger.info("Extract necessary fields and prepare disease ids for expansion")
    val df = getRawEvidence.transform(prepareDiseaseIds)

    val path = config.outputPath.resolve("evidence.parquet")
    df.write.format("parquet").mode("overwrite").save(path.toString)
    logger.info(s"Saved preprocessed evidence data to $path")

    logger.info("Preprocessing complete")
    this
  }

  def runScoring(targets: Option[Seq[Any]] = None, diseases: Option[Seq[Any]] = None): Pipeline = {
    val logger = Logger
    logger.info("Beginning scoring pipeline")

    logger.info("Fetching evidence data and computing resource scores")
    var df = getPreprocessedEvidence.transform(computeResourceScores)

    // Apply target filters if necessary
    if (targets.isDefined) {
      logger.info("Applying target filters")
      df = df.filter($"target_id".isin(targets.get: _*))
    }

    logger.info("Exploding records across disease ontology")
    df = df.transform(explodeByDiseaseId)

    // Apply disease filters if necessary (after explosion to rows)
    if (diseases.isDefined) {
      logger.info("Applying disease filters")
      df = df.filter($"disease_id".isin(diseases.get: _*))
    }

    logger.info("Computing harmonic sum factors with source specific weights")
    df = df.transform(computeSourceScores)

    if (config.saveEvidenceScores) {
      val path = config.outputPath.resolve("score_evidence.parquet")
      df.write.format("parquet").mode("overwrite").save(path.toString)
      logger.info(s"Saved evidence-level scores to $path")
    }

    logger.info("Computing source level scores")
    val dfs = df.transform(aggregateSourceScores)

    logger.info("Computing association level scores")
    val dfa = dfs.transform(aggregateAssociationScores)

    var path = config.outputPath.resolve("score_source.parquet")
    dfs.write.format("parquet").mode("overwrite").save(path.toString)
    logger.info(s"Saved source-level data to $path")

    path = config.outputPath.resolve("score_association.parquet")
    dfa.write.format("parquet").mode("overwrite").save(path.toString)
    logger.info(s"Saved source-level data to $path")

    logger.info("Scoring complete")
    this
  }
}
