package com.relatedsciences.opentargets.etl
import com.relatedsciences.opentargets.etl.configuration.Configuration.Config
import com.relatedsciences.opentargets.etl.pipeline.Components.State
import com.relatedsciences.opentargets.etl.pipeline.{EvidencePreparationPipeline, PipelineState, ScoringCalculationPipeline, ScoringPreparationPipeline}
import com.typesafe.scalalogging.LazyLogging
import enumeratum._
import enumeratum.EnumEntry._
import org.apache.spark.sql.SparkSession
import java.util.{List => JList}

import com.relatedsciences.opentargets.etl.pipeline.Pipeline.{Spec, SpecProvider}

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
    // TODO: make this configurable?
    logger.info(s"Pipeline data structure summaries:\n${state.summaries}")
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
    case object PrepareEvidence
      extends CommandEnum(
        CLIOpts("Execute evidence validation and normalization"),
        (ss, c) =>
          new PipelineCommand(ss, c) {
            override def spec(): Spec = new EvidencePreparationPipeline(ss, c).spec()
          }
      )


  }

}
