package com.relatedsciences.opentargets.pipeline
import org.scalatest.FunSuite

class PipelineSuite extends FunSuite with SparkSessionWrapper {

  test("pipeline can run end-to-end") {
    // TODO: hook this up to recent data exports with exptected scores for comparison
//    val config = TestUtils.getPipelineConfig()
//    new Pipeline(spark, config)
//      .runPreprocessing()
//      .runScoring()
  }

}
