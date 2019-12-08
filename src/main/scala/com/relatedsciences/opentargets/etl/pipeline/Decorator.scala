package com.relatedsciences.opentargets.etl.pipeline

import com.relatedsciences.opentargets.etl.Utilities.Stopwatch
import com.relatedsciences.opentargets.etl.configuration.Configuration.PipelineDecoratorConfig
import com.relatedsciences.opentargets.etl.pipeline
import com.relatedsciences.opentargets.etl.pipeline.Components.{Operation, S}
import com.typesafe.scalalogging.LazyLogging
import enumeratum.EnumEntry.Hyphencase
import enumeratum.{Enum, EnumEntry}
import org.apache.spark.sql.Dataset

import scala.collection.immutable

abstract class Decorator(config: PipelineDecoratorConfig) {
  def apply[A](op: Operation[A]): Operation[A]
}

object Decorator extends LazyLogging {

  sealed abstract class DecoratorEnum(
      val factory: PipelineDecoratorConfig => Decorator
  ) extends EnumEntry
      with Hyphencase

  object DecoratorEnum extends Enum[DecoratorEnum] {

    val values: immutable.IndexedSeq[DecoratorEnum] = findValues

    case object Time           extends DecoratorEnum(c => new Decorator.TimingDecorator(c))
    case object Log            extends DecoratorEnum(c => new Decorator.LoggingDecorator(c))
    case object Reference extends DecoratorEnum(c => new pipeline.Decorator.ReferenceDecorator(c))
    case object DatasetSummary extends DecoratorEnum(c => new Decorator.DatasetSummaryDecorator(c))

  }

  class TimingDecorator(config: PipelineDecoratorConfig) extends Decorator(config) {
    override def apply[A](op: Operation[A]): Operation[A] = new Operation[A] {
      val scope: Seq[String] = op.scope
      def run: S => A = (s: S) => {
        val watch = Stopwatch.start()
        val o     = op.run(s)
        s.addTime(op.scope.last, watch.elapsed())
        o
      }
    }
  }

  class LoggingDecorator(config: PipelineDecoratorConfig) extends Decorator(config) {
    override def apply[A](op: Operation[A]): Operation[A] = new Operation[A] {
      val scope: Seq[String] = op.scope
      def run: S => A = (s: S) => {
        logger.info(s"Executing operation '${op.scope.last}'")
        val o = op.run(s)
        logger.info(s"Operation '${op.scope.last}' complete")
        o
      }
    }
  }

  class DatasetSummaryDecorator(config: PipelineDecoratorConfig) extends Decorator(config) {
    override def apply[A](op: Operation[A]): Operation[A] = new Operation[A] {
      val scope: Seq[String] = op.scope
      def run: S => A = (s: S) => {
        val o = op.run(s)
        if (o.isInstanceOf[Dataset[_]]) {
          val df = o.asInstanceOf[Dataset[_]]
          s.addSummary(op.scope.last, df.count(), df.schema.treeString)
        }
        o
      }
    }
  }

  class ReferenceDecorator(config: PipelineDecoratorConfig) extends Decorator(config) {
    override def apply[A](op: Operation[A]): Operation[A] = new Operation[A] {
      val scope: Seq[String] = op.scope
      def run: S => A = (s: S) => {
        val o = op.run(s)
        s.addReference(op.scope.last, o)
        o
      }
    }
  }
}
