package com.relatedsciences.opentargets.pipeline

import org.apache.spark.sql.Row

import scala.collection.mutable

object Scoring {

  class UnsupportedDataTypeException(message: String) extends Exception(message)

  /**
  * Compute the association score for a single evidence string
   *
   * @param typeId type associated with data (e.g. rna_expression, somatic_mutation, genetic_association)
   * @param sourceId source of data for the given type (e.g. gwas_catalog, twentythreeandme, sysbio)
   * @param data evidence data
   * @return score in [0, 1] unless insufficient data was present for computation
   */
  def score(typeId: String, sourceId: String, data: Row): Option[Double] = {
    Scorer.byTypeId(typeId) match {
      case Some(scorer) => scorer.score(data)
      case None => throw new UnsupportedDataTypeException(
        s"Failed to find scoring implementation for data type '$typeId'")
    }
  }

  class Record(row: Row) {
    def get[T](field: FieldName.Value): Option[T] = {
      val f = FieldName.flatName(field)
      if (row.isNullAt(row.fieldIndex(f))) None else Some(row.getAs[T](f))
    }
    def exists(field: FieldPath.Value): Boolean = {
      row.getAs(FieldPath.flatName(field))
    }
    def row(): Row = row
  }

  abstract class Scorer {
    def score(data: Row): Option[Double] = {
      score(new Record(data))
    }

    def score(data: Record): Option[Double]
  }

  class KnownDrugScorer extends Scorer {
    override def score(data: Record): Option[Double] = {
      val v = data.get[Double](FieldName.evidence$drug2clinic$resource_score$value).get *
        data.get[Long](FieldName.evidence$target2drug$resource_score$value).get
      Some(v)
    }
  }

  class RnaExpressionScorer extends Scorer {
    override def score(data: Record): Option[Double] = {
      val p_value = data.get[Double](FieldName.evidence$resource_score$value).get
      val log2_fold_change =
        data.get[Double](FieldName.evidence$log2_fold_change$value).get
      val prank = data.get[Long](FieldName.evidence$log2_fold_change$percentile_rank).get / 100.0
      val fold_scale_factor = Math.abs(log2_fold_change) / 10.0
      val score = p_value * fold_scale_factor * prank
      Some(Math.min(score, 1.0))
    }
  }

  class AnimalModelScorer extends Scorer {
    override def score(data: Record): Option[Double] = {
      data.get[Double](FieldName.evidence$disease_model_association$resource_score$value)
    }
  }

  class SomaticMutationScorer extends Scorer {

    def processMutation(mutation: Row): Option[(Long, Long)] = {
      if (!mutation.schema.fieldNames.contains("number_samples_with_mutation_type")){
        return None
      }
      if (mutation.isNullAt(mutation.fieldIndex("number_samples_with_mutation_type"))){
        return None
      }
      Some((mutation.getAs[Long]("number_samples_with_mutation_type"),
        mutation.getAs[Long]("number_mutated_samples")))
    }

    override def score(data: Record): Option[Double] = {
      var frequency = 1.0
      val mutations = data.get[Seq[Row]](FieldName.evidence$known_mutations)
      if (mutations.isDefined && mutations.get.nonEmpty) {
        var sampleTotalCoverage = 1.0
        var maxSampleSize = 1.0
        for (t <- mutations.get.flatMap(processMutation)) {
          sampleTotalCoverage += t._1
          maxSampleSize = Math.max(maxSampleSize, t._2)
        }
        frequency = Utilities.normalize(sampleTotalCoverage / maxSampleSize, (0.0, 9.0), (0.5, 1.0))
      }
      Some(data.get[Double](FieldName.evidence$resource_score$value).get * frequency)
    }
  }

  /**
  * Static field enum representing flat paths to values within evidence strings
   *
   * Note: These fields are used in the extraction pipeline to form a union of columns
   * to extract from raw evidence and as such, any field necessary for a score
   * calculation must be registered here.
   */
  object FieldName extends Enumeration {
    val evidence$drug2clinic$resource_score$value,
    evidence$target2drug$resource_score$value, evidence$log2_fold_change$value,
    evidence$log2_fold_change$percentile_rank, evidence$resource_score$value,
    evidence$disease_model_association$resource_score$value, evidence$known_mutations =
      Value

    def pathName(value: Value): String =
      value.toString.replace("$", ".")

    def flatName(value: Value): String =
      value.toString.replace("$", "_")
  }

  /**
   * Static field enum representing flat paths for which existence of information at that path must be inferred
   *
   * Note: These fields are used in the extraction pipeline to form a union of columns
   * to extract from raw evidence and as such, any field necessary for a score
   * calculation must be registered here.
   */
  object FieldPath extends Enumeration {
    val evidence$gene2variant, evidence$known_mutations = Value


    def pathName(value: Value): String =
      value.toString.replace("$", ".")

    def flatName(value: Value): String =
      value.toString.replace("$", "_") + "_exists"
  }

  /**
  * Scoring implementations by data type
   */
  object Scorer extends Enumeration {

    protected case class Val(name: String, scorer: Scorer) extends super.Val

    import scala.language.implicitConversions

    implicit def valueToVal(x: Value): Val = x.asInstanceOf[Val]

    def byTypeId(id: String): Option[Scorer] = {
      Scorer.values.find(_.name == id).map(_.scorer)
    }

    val KnownDrug = Val("known_drug", new KnownDrugScorer)
    val RnaExpression = Val("rna_expression", new RnaExpressionScorer)
    val AnimalModel = Val("animal_model", new AnimalModelScorer)
    val SomaticMutation = Val("somatic_mutation", new SomaticMutationScorer)
  }

}
