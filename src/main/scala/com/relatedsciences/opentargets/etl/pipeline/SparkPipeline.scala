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
}
