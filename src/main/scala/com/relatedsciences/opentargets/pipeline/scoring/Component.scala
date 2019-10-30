package com.relatedsciences.opentargets.pipeline.scoring

import org.apache.spark.sql.types.{DoubleType, StringType, StructType}

case class Component(name: String, score: Double, weight: Double)

object Component {
  val Schema: StructType = new StructType()
    .add("name", StringType, nullable = false)
    .add("score", DoubleType, nullable = false)
    .add("weight", DoubleType, nullable = false)

  object ComponentName extends Enumeration {
    val resource_score, sample_size, drug2clinic, target2drug, disease_model_association, p_value,
    log2_fold_scale_factor, log2_fold_change_rank, mutation_frequency, v2d_score, g2v_score = Value
  }
}
