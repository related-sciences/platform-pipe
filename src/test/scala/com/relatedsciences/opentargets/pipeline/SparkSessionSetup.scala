package com.relatedsciences.opentargets.pipeline
import org.apache.spark.sql.SparkSession

trait SparkSessionSetup {
  def withSpark(testMethod: (SparkSession) => Any) {
    val spark = SparkSession.builder
      .appName("OT Scoring Tests")
      .config("spark.master", "local[*]")
      .getOrCreate()
    try {
      testMethod(spark)
    }
    finally spark.stop()
  }
}