package com.relatedsciences.opentargets.etl.pipeline

import com.relatedsciences.opentargets.etl.configuration.Configuration.Config
import com.relatedsciences.opentargets.etl.pipeline.Pipeline.SpecProvider
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Column, DataFrame, DataFrameWriter, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel

abstract class SparkPipeline(ss: SparkSession, config: Config)
    extends SpecProvider
    with LazyLogging {
  import ss.implicits._
  type WriterConfigurator = DataFrameWriter[Row] => DataFrameWriter[Row]

  // Return sensible defaults, possibly modified by configuration if necessary in the future
  private def defaultWriterConfigurator(): WriterConfigurator =
    (writer: DataFrameWriter[Row]) => writer.format("parquet").mode("overwrite")

  def save(
      df: DataFrame,
      path: String,
      writerConfigurator: Option[WriterConfigurator] = None
  ): DataFrame = {
    val writer = writerConfigurator.getOrElse(defaultWriterConfigurator())(df.write)
    writer.save(path.toString)
    logger.info(s"Saved data to '$path'")
    df
  }

  def assertSchemasEqual(sourceDF: DataFrame, msg: String = "")(df: DataFrame): DataFrame = {
    if (config.pipeline.enableAssertions) {
      val equal = sourceDF.schema == df.schema
      if (!equal) {
        logger.error(s"[schema assertion error] Schema for data frame 1:\n")
        sourceDF.printSchema
        logger.error(s"[schema assertion error] Schema for data frame 2:\n")
        df.printSchema
      }
      val prefix = if (msg.isEmpty) "Data frame schemas not equal" else msg
      assert(equal, prefix + "; see schemas printed above for comparison")
    }
    df
  }

  def summarizeValidationErrors(summaryPath: String, errorsPath: String)(df: DataFrame): DataFrame = {
    if (config.pipeline.enableSummaries) {
      // Make sure to persist the data frame first as not doing so leads to re-computation
      // for each of the following operations, as well any others downstream
      // * At TOW, this reduces computation time from ~3 hrs to ~45 mins
      df.persist(StorageLevel.DISK_ONLY)
      save(df.groupBy("sourceID", "reason").count(), summaryPath)
      save(df.filter(!col("is_valid")), errorsPath)
    }
    df
  }

}
