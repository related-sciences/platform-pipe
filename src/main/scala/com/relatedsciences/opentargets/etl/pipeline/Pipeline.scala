package com.relatedsciences.opentargets.etl.pipeline

import java.util.NoSuchElementException

import com.relatedsciences.opentargets.etl.configuration.Configuration.Config
import com.relatedsciences.opentargets.etl.pipeline.Components.{Operation, Source, State}
import com.relatedsciences.opentargets.etl.pipeline.Decorator.DecoratorEnum
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Success}

class Pipeline[A](spec: Operation[A], config: Config) {
  def run(state: State): A = {
    spec.run(state)
  }
}

object Pipeline extends LazyLogging {

  class Builder(config: Config) {

    lazy private val decorators = {
      val decorators = config.pipeline.decorators.filter(t => t._2.enabled)
      logger.debug(s"Initializing pipeline with decorators: ${decorators.keys}")
      try {
        Success(decorators.map(t => DecoratorEnum.withName(t._1).enumEntry.factory(t._2)))
      } catch {
        case e: NoSuchElementException =>
          Failure(
            e.initCause(
              new NoSuchElementException(
                "At least one of the provided decorator implementation names is not valid " +
                  "(the offending name should be removed or updated in the application config)"
              )
            )
          )
      }
    }

    private def decorate[A](op: Operation[A]): Operation[A] = {
      // Wrap the operation in all configured decorators
      // * This results in op = dN(d2(d1(operation))) where decorators are applied in given sort order
      Function.chain(decorators.get.map(d => d.apply[A] _).toSeq)(op)
    }

    /**
      * Operation encapsulation model used to inject logic requiring access to
      * global application configuration properties
      */
    class Op[A](val operation: Operation[A]) {
      def andThen[B](name: String, f: A => B): Op[B] = {
        // Compose the new functor from the provide function and decorate the result
        Op(decorate(operation.map(name, f)))
      }

      def stop[B](name: String, f: A => B): Pipeline[B] = {
        new Pipeline(decorate(operation.map(name, f)), config)
      }

      def end(name: String = "end"): Pipeline[Unit] = {
        stop(name, _ => ())
      }
    }

    object Op {
      def apply[A](op: Operation[A]) = new Op(op)
    }

    def start[A](name: String, f: () => A): Op[A] = {
      Op(decorate(Source.create[A](name, f)))
    }

  }

  object Builder {
    def apply(config: Config) = new Builder(config)
  }

}
