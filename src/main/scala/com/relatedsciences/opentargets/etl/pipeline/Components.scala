package com.relatedsciences.opentargets.etl.pipeline

import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration.Duration

object Components extends LazyLogging {

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
  }

  object Source {
    def create[A](name: String, f: () => A): Operation[A] = {
      new Operation[A] {
        val scope: Seq[String] = Seq(name)
        def run: (S) => A      = (_) => f()
      }
    }
  }

  case class Time(name: String, duration: Duration)
  case class Summary(name: String, count: Long, schema: String)
  case class Reference(name: String, value: Any)
  trait State {
    def times: List[Time]
    def summaries: List[Summary]
    def references: List[Reference]
    def addTime(name: String, time: Duration)
    def addSummary(name: String, count: Long, schema: String)
    def addReference(name: String, value: Any)
  }

  type S    = State

}
