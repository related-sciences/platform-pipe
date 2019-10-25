package com.relatedsciences.opentargets.pipeline

import org.apache.spark.sql.Row

object Scoring {

  def score(typeId: String, sourceId: String, data: Row): Double = {
    Scorer.byTypeId(typeId) match {
      case Some(scorer) => scorer.score(data)
      case None => throw new RuntimeException(s"Failed to find scoring implementation for data type '$typeId'")
    }
  }

  class Record(row: Row) {
    def get[T](field: FieldName.Value): T = {
      row.getAs[T](FieldName.flatName(field))
    }
  }

  abstract class Scorer {
    def score(data: Row): Double = {
      score(new Record(data))
    }

    def score(data: Record): Double
  }

  class KnownDrugScorer extends Scorer {
    override def score(data: Record): Double = {
      data.get[Double](FieldName.evidence$drug2clinic$resource_score$value) *
        data.get[Long](FieldName.evidence$target2drug$resource_score$value)
    }
  }

  class RnaExpressionScorer extends Scorer {
    override def score(data: Record): Double = {
      val p_value = data.get[Double](FieldName.evidence$resource_score$value)
      val log2_fold_change =
        data.get[Double](FieldName.evidence$log2_fold_change$value)
      val prank = data.get[Long](
        FieldName.evidence$log2_fold_change$percentile_rank
      ) / 100.0
      val fold_scale_factor = Math.abs(log2_fold_change) / 10.0
      val score = p_value * fold_scale_factor * prank
      Math.min(score, 1.0)
    }
  }

  object FieldName extends Enumeration {
    val evidence$drug2clinic$resource_score$value,
    evidence$target2drug$resource_score$value, evidence$log2_fold_change$value,
    evidence$log2_fold_change$percentile_rank, evidence$resource_score$value =
      Value

    def pathName(value: Value): String =
      value.toString.replace("$", ".")

    def flatName(value: Value): String =
      value.toString.replace("$", "_")
  }

  object Scorer extends Enumeration {

    protected case class Val(name: String, scorer: Scorer) extends super.Val

    import scala.language.implicitConversions

    implicit def valueToVal(x: Value): Val = x.asInstanceOf[Val]

    def byTypeId(id: String): Option[Scorer] = {
      Scorer.values.find(_.name == id).map(_.scorer)
    }

    val KnownDrug = Val("known_drug", new KnownDrugScorer)
    val RnaExpression = Val("rna_expression", new RnaExpressionScorer)
  }

}
