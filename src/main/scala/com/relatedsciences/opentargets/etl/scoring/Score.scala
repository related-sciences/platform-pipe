package com.relatedsciences.opentargets.etl.scoring

import com.relatedsciences.opentargets.etl.scoring.Component.ComponentName
import org.apache.spark.sql.types.{ArrayType, DoubleType, StructType}

case class Score(score: Double, components: Array[Component])

object Score {

  val Schema: StructType = new StructType()
    .add("score", DoubleType, nullable = false)
    .add("components", ArrayType(Component.Schema, containsNull = false), nullable = true)

  def from(components: Array[Component], transform: Double => Double = identity): Score = {
    val score = transform(components.map(v => v.weight * v.score).product)
    new Score(score, components)
  }

  def using(weights: Map[ComponentName.Value, Double]): ScoreBuilder = {
    new ScoreBuilder(weights)
  }
}

class ScoreBuilder(weights: Map[ComponentName.Value, Double]) {
  var items: List[Component] = List()

  def add(component: ComponentName.Value, value: Double): ScoreBuilder = {
    val weight = this.weights(component)
    this.items = new Component(component.toString, value, weight) :: this.items
    this
  }
  def get(transform: Double => Double = identity): Score = {
    if (this.items.isEmpty) {
      throw new IllegalStateException("At least one score component must be added")
    }
    Score.from(this.items.toArray, transform)
  }
  def calculate(transform: Double => Double = identity): Option[Score] = {
    Some(this.get(transform))
  }
}
