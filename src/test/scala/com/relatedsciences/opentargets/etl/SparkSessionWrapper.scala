package com.relatedsciences.opentargets.etl
import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

  lazy val ss: SparkSession = {
    // Settings lifted from: https://github.com/holdenk/spark-testing-base/blob/79eef40cdab48ee7aca8902754e3c456f569eea6/core/src/main/1.3/scala/com/holdenkarau/spark/testing/SparkContextProvider.scala
    SparkSession
      .builder()
      .master("local[2]")
      .appName("test")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .getOrCreate()
  }

}
