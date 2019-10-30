package com.relatedsciences.opentargets.pipeline.scoring

import com.relatedsciences.opentargets.pipeline.scoring.Component.ComponentName

case class Parameters(weights: Map[ComponentName.Value, Double])

object Parameters {
  def default(): Parameters = {
    val weights = ComponentName.values.map((_, 1.0)).toMap
    new Parameters(weights)
  }
}
