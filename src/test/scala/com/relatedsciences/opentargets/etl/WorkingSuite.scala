package com.relatedsciences.opentargets.etl

import com.relatedsciences.opentargets.etl.pipeline.{EvidenceValidationPipeline, PipelineState}
import org.apache.spark.sql.Dataset
import org.scalatest.FunSuite

class WorkingSuite extends FunSuite with SparkSessionWrapper {

  test("current working component") {
    val state = PipelineState()
    new EvidenceValidationPipeline(ss, TestUtils.primaryTestConfig).spec().run(state)
    val refs = state.references.map(v => v.name -> v.value).toMap
    val df = refs("parseEvidenceData").asInstanceOf[Dataset[_]]
    println(df.show(10))
    println(df.count())
    println(df.first())
  }

}
