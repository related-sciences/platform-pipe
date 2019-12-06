package com.relatedsciences.opentargets.etl.pipeline

import com.relatedsciences.opentargets.etl.pipeline.Components.State
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration

class PipelineState extends State with LazyLogging {
  case class Time(name: String, duration: Duration)
  var times = new ListBuffer[Time]()

  override def addTime(name: String, duration: Duration): Unit = {
    times += Time(name, duration)
  }
}
