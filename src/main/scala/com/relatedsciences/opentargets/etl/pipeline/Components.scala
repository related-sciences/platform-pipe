package com.relatedsciences.opentargets.etl.pipeline

import com.relatedsciences.opentargets.etl.Utilities.Stopwatch
import com.relatedsciences.opentargets.etl.configuration.Configuration.PipelineDecoratorConfig
import com.relatedsciences.opentargets.etl.pipeline.Components.Decorator.TimingDecorator
import com.typesafe.scalalogging.LazyLogging
import enumeratum._
import enumeratum.EnumEntry._

import scala.collection.immutable
import scala.concurrent.duration.Duration

object Components extends LazyLogging {

  trait State {
    def addTime(name: String, time: Duration)
  }
  type S    = State
  type Spec = Pipeline[Unit]
  trait SpecProvider {
    def spec(): Spec
  }

  abstract class Operation[A] {
    val scope: Seq[String]
    def run: S => A

    def map[B](name: String, f: A => B): Operation[B] = {
      val r = this.run
      val s = this.scope
      new Operation[B] {
        val scope: Seq[String] = s :+ name
        def run: S => B        = (s: S) => f(r(s))
      }
    }

    def pipe(f: Operation[A] => Operation[A]): Operation[A] = f(this)
  }

  abstract class Decorator(config: PipelineDecoratorConfig) {
    def apply[A](op: Operation[A]): Operation[A]
  }
  sealed abstract class DecoratorEnum(
      val name: String,
      val factory: PipelineDecoratorConfig => Decorator
  ) extends EnumEntry
      with Hyphencase

  object DecoratorEnum extends Enum[DecoratorEnum] {

    val values: immutable.IndexedSeq[DecoratorEnum] = findValues

    case object Time extends DecoratorEnum("time", c => new Decorator.TimingDecorator(c))
    case object Log  extends DecoratorEnum("log", c => new Decorator.LoggingDecorator(c))

  }

  object Decorator {

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
  }

  object Source {
    def create[A](name: String, f: () => A): Operation[A] = {
      new Operation[A] {
        val scope: Seq[String] = Seq(name)
        def run: (S) => A      = (_) => f()
      }
    }
  }

}
