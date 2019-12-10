package com.relatedsciences.opentargets.etl
import com.relatedsciences.opentargets.etl.configuration.Configuration.{Config, PipelineDecoratorConfig}
import com.relatedsciences.opentargets.etl.pipeline.{Pipeline, PipelineState}
import org.scalatest.FunSuite

case class TestRecord(s: String, i: Int)

class ComponentsSuite extends FunSuite with SparkSessionWrapper {
  import ss.implicits._

  def getConfig() = {
    val config = TestUtils.primaryTestConfig
    val decorators = config.pipeline.decorators + ("dataset-summary" -> PipelineDecoratorConfig(enabled=true))
    val pipeline = config.pipeline.copy(decorators=decorators)
    config.copy(pipeline=pipeline)
  }

  private def pipeline1(config: Config = getConfig()) = {
    Pipeline
      .Builder(config)
      .start("createValue", () => "2")
      .andThen("toInt", v => v.toInt)
      .andThen("square", v => v * v)
      .andThen("toString", v => v.toString)
      .stop("return", v => v)
  }

  private def pipeline2(config: Config = getConfig(), ct: Int) = {
    Pipeline
      .Builder(config)
      .start("createCount", () => ct)
      .andThen("toDF", v => Range(0, v).map(i => TestRecord(i.toString, i)).toDS())
      .andThen("filterDF", v => v.filter($"i" < 10))
      .stop("return", v => v)
  }

  test("simple pipeline models") {
    val res = pipeline1().run(new PipelineState)
    assertResult("4")(res)
  }

  test("basic pipeline decorators") {
    val state = new PipelineState
    pipeline1().run(state)

    // Check timers
    val names = state.times.map(t => t.name)
    assert(
      names.distinct.size == names.size,
      "Operation names recorded are not unique, found $names"
    )
    assert(names.size == 5, "Timings not recorded for all operations")

  }

  test("dataset summary pipeline decorator") {
    val ct    = 25
    val state = new PipelineState
    val df    = pipeline2(ct = ct).run(state)
    assertResult(10)(df.count())
    assertResult(3)(state.summaries.size)
    assertResult(ct)(state.summaries(0).count)
    assertResult(10)(state.summaries(1).count)
  }

  test("invalid configuration for decorators") {
    assertThrows[NoSuchElementException]({
      val config = getConfig()
      val decorators = config.pipeline.decorators + ("some_invalid_decorator" -> PipelineDecoratorConfig(enabled=true))
      val pipeline = config.pipeline.copy(decorators=decorators)
      pipeline1(config.copy(pipeline=pipeline)).run(new PipelineState)
    })
  }

}
