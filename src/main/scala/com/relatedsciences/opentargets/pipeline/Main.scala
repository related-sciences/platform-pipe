/**
  * Scoring pipeline execution for spark-submit
  * Usage:
  * /usr/spark-2.4.4/bin/spark-submit \
  * --driver-memory 12g \
  * --class "com.relatedsciences.opentargets.pipeline.Main" \
  * target/scala-2.12/ot-scoring_2.12-0.1.jar
  */
package com.relatedsciences.opentargets.pipeline
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]) {
    val spark = SparkSession.builder
      .appName("OT Scoring")
      .config("spark.master", "local[*]")
      .getOrCreate()
    new Pipeline(spark)
      .runPreprocessing()
      .runScoring()
    spark.stop()
  }
}
