package com.relatedsciences.opentargets.etl.pipeline

import com.relatedsciences.opentargets.etl.configuration.Configuration.Config
import com.relatedsciences.opentargets.etl.pipeline.Pipeline.SpecProvider
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}

abstract class SparkPipeline(ss: SparkSession, config: Config)
    extends SpecProvider
    with LazyLogging {

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

  def assertSizesEqual(df1: DataFrame, df2: DataFrame, msg: String): Unit = {
    if (config.pipeline.enableAssertions) {
      val expected = df1.count()
      val actual   = df2.count()
      assert(expected == actual, msg.format(expected, actual))
    }
  }

  def assertSchemasEqual(df1: DataFrame, df2: DataFrame, msg: String = ""): Unit = {
    if (config.pipeline.enableAssertions) {
      // Is there a better way to do this?
      val equal = df1.schema.toString == df2.schema.toString
      if (!equal) {
        logger.error(s"[schema assertion error] Schema for data frame 1:\n")
        df1.printSchema
        logger.error(s"[schema assertion error] Schema for data frame 2:\n")
        df2.printSchema
      }
      val prefix = if (msg.isEmpty) "Data frame schemas not equal" else msg
      assert(equal, prefix + "; see schemas printed above for comparison")
    }
  }
}
