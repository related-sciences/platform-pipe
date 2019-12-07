package com.relatedsciences.opentargets.etl
import com.relatedsciences.opentargets.etl.configuration.Configuration.Config
import com.relatedsciences.opentargets.etl.pipeline.Components.{Spec, SpecProvider, State}
import com.relatedsciences.opentargets.etl.pipeline.{
  PipelineState,
  ScoringCalculationPipeline,
  ScoringPreparationPipeline
}
import com.typesafe.scalalogging.LazyLogging
import enumeratum._
import enumeratum.EnumEntry._
import org.apache.spark.sql.SparkSession
import java.util.{List => JList}

import scala.collection.immutable

abstract class Command(ss: SparkSession, config: Config) extends LazyLogging {
  def run(): Unit
}

abstract class PipelineCommand(ss: SparkSession, config: Config)
    extends Command(ss, config)
    with SpecProvider {

  override def run(): Unit = {
    val state = run(new PipelineState)
    summarize(state)
  }

  def run(state: State): State = {
    spec().run(state)
    state
  }

  def summarize(state: State): Unit = {
    logger.info(
      s"Pipeline complete; Summary:\n" +
        s"\ttimes: ${state.times}"
    )
  }
}

object Command {
  case class CLIOpts(text: String)
  sealed abstract class CommandEnum(
      val opts: CLIOpts,
      val factory: (SparkSession, Config) => Command
  ) extends EnumEntry
      with Hyphencase

  object CommandEnum extends Enum[CommandEnum] {

    val values: immutable.IndexedSeq[CommandEnum] = findValues

    case object PrepareScores
        extends CommandEnum(
          CLIOpts("Prepare raw evidence records by expanding indirect disease associations"),
          (ss, c) =>
            new PipelineCommand(ss, c) {
              override def spec(): Spec = new ScoringPreparationPipeline(ss, c).spec()
            }
        )
    case object CalculateScores
        extends CommandEnum(
          CLIOpts("Execute evidence scoring and aggregation"),
          (ss, c) =>
            new PipelineCommand(ss, c) {
              override def spec(): Spec = new ScoringCalculationPipeline(ss, c).spec()
            }
        )

    case object DownloadEvidence
      extends CommandEnum(
        CLIOpts(
          """Download evidence files from GS
            |NOTE: This is not the recommended way to source these files in production but it is useful for testing
            |""".stripMargin),
        (ss, c) =>
          new Command(ss, c) {
            def run(): Unit = {
              import scala.collection.JavaConverters.iterableAsScalaIterable
              // See here for yaml parsing example: https://github.com/related-sciences/ot-scoring/blob/4fd9520623d8bab8d4cff9b353b06144c06ef43d/src/main/scala/com/relatedsciences/opentargets/pipeline/Configuration.scala#L29
              val config = Utilities.loadYaml(c.externalConfig.mrtargetData)
              val urls = iterableAsScalaIterable(config.get("input-file").asInstanceOf[JList[String]])
              // TODO: Download each file while respecting argument to potentially limit size
            }
          }
      )
  }

}
