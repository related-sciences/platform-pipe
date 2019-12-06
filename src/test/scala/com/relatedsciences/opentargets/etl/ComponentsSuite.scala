package com.relatedsciences.opentargets.etl
import com.relatedsciences.opentargets.etl.configuration.Configuration.Config
import com.relatedsciences.opentargets.etl.pipeline.{Pipeline, PipelineState}
import org.scalatest.FunSuite

class ComponentsSuite extends FunSuite {

  def pipeline1(config: Config = TestUtils.primaryTestConfig) = {
    Pipeline
      .Builder(config)
      .start("createValue", () => "2")
      .andThen("toInt", v => v.toInt)
      .andThen("square", v => v * v)
      .andThen("toString", v => v.toString)
      .stop("exit", v => v)
  }

  test("simple pipeline models") {
    val res = pipeline1().run(new PipelineState)
    assertResult("4")(res)
  }

  test("pipeline decorators") {
    val state = new PipelineState
    pipeline1().run(state)
    val names = state.times.map(t => t.name)
    assert(
      names.distinct.size == names.size,
      "Operation names recorded are not unique, found $names"
    )
    assert(names.size == 5, "Timings not recorded for all operations")
  }

  test("invalid configuration for decorators") {
    assertThrows[NotImplementedError]({
      val config = TestUtils.getConfig("config/application-bad-decorator.conf")
      pipeline1(config).run(new PipelineState)
    })
  }

}
