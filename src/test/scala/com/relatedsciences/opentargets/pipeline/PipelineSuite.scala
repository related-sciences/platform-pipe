package com.relatedsciences.opentargets.pipeline
import org.scalatest.FunSuite

class PipelineSuite extends FunSuite with SparkSessionSetup {

  test("pipeline can run end-to-end") {
    withSpark((spark) => {
      val config = TestUtils.getPipelineConfig()
      new Pipeline(spark, config)
        .runPreprocessing()
        .runScoring()
    })
  }

}
