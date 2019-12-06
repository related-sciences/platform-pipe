package com.relatedsciences.opentargets.etl.pipeline

import com.relatedsciences.opentargets.etl.pipeline.Components.State
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration

class PipelineState extends State with LazyLogging {
  case class Time(name: String, duration: Duration)
  var times = new ListBuffer[Time]()

  case class Summary(name: String, count: Long, schema: String)
  var summaries = new ListBuffer[Summary]

  override def addTime(name: String, duration: Duration): Unit = {
    times += Time(name, duration)
  }

  override def addSummary(name: String, count: Long, schema: String): Unit = {
    summaries += Summary(name, count, schema)
  }
}
