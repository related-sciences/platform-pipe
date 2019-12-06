package com.relatedsciences.opentargets.etl
import com.relatedsciences.opentargets.etl.configuration.Configuration.Config
import com.relatedsciences.opentargets.etl.pipeline.{
  PipelineState,
  ScoringCalculationPipeline,
  ScoringPreparationPipeline
}
import com.typesafe.scalalogging.LazyLogging
import enumeratum._
import enumeratum.EnumEntry._
import org.apache.spark.sql.SparkSession

import scala.collection.immutable

abstract class Command(ss: SparkSession, config: Config) extends LazyLogging {
  def run()
}

object Command {
  sealed abstract class CommandEnum(val factory: (SparkSession, Config) => Command)
      extends EnumEntry
      with Hyphencase

  object CommandEnum extends Enum[CommandEnum] {

    val values: immutable.IndexedSeq[CommandEnum] = findValues

    case object RunScoringPipeline extends CommandEnum((ss, c) => new RunScoringPipeline(ss, c))
    case object Command2           extends CommandEnum((ss, c) => new Command2(ss, c))
    // case object ShoutGoodBye extends Command with Uppercase

  }

  class RunScoringPipeline(ss: SparkSession, config: Config) extends Command(ss, config) {
    override def run(): Unit = {
      val state = new PipelineState
      new ScoringPreparationPipeline(ss: SparkSession, config: Config).spec().run(state)
      new ScoringCalculationPipeline(ss: SparkSession, config: Config).spec().run(state)
      logger.info(
        s"Pipeline complete; Summary:\n" +
          s"\ttimes: ${state.times}"
      )
    }
  }

  class Command2(ss: SparkSession, config: Config) extends Command(ss, config) {
    override def run(): Unit = println("In command2")
  }

}
