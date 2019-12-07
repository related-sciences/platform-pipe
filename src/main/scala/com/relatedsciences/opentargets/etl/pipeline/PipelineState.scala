package com.relatedsciences.opentargets.etl.pipeline

import com.relatedsciences.opentargets.etl.pipeline.Components.{State, Time, Summary}
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration

class PipelineState extends State with LazyLogging {
  private var timesL     = new ListBuffer[Time]()
  private var summariesL = new ListBuffer[Summary]()

  override def times: List[Time] = {
    timesL.toList
  }

  override def summaries: List[Summary] = {
    summariesL.toList
  }

  override def addTime(name: String, duration: Duration): Unit = {
    timesL += Time(name, duration)
  }

  override def addSummary(name: String, count: Long, schema: String): Unit = {
    summariesL += Summary(name, count, schema)
  }
}
