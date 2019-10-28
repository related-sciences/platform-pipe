package com.relatedsciences.opentargets.pipeline

import com.relatedsciences.opentargets.pipeline.schema.Fields.{FieldName, FieldPath}
import org.apache.spark.sql.Row
import schema.DataType
import schema.DataSource

object Scoring {

  class UnsupportedDataTypeException(message: String) extends Exception(message)
  class UnsupportedResourceScoreTypeException(message: String)
      extends Exception(message)
  class InvalidRecordException(message: String) extends Exception(message) {
    def this(message: String, cause: Throwable){
      this(message)
      initCause(cause)
    }
  }

  /**
    * Compute the association score for a single evidence string
    *
    * @param id identifier for evidence string
    * @param typeId type associated with data (e.g. rna_expression, somatic_mutation, genetic_association)
    * @param sourceId source of data for the given type (e.g. gwas_catalog, twentythreeandme, sysbio)
    * @param data evidence data
    * @return score in [0, 1] unless insufficient data was present for computation
    */
  def score(id: String, typeId: String, sourceId: String, data: Row): Option[Double] = {
    val record: Record = new Record(id, typeId, sourceId, data)
    Scorer.byTypeId(typeId) match {
      case Some(scorer) => try {
        scorer.score(record)
      } catch {
        case e: Exception => throw new InvalidRecordException(s"Failed record: $record", e)
      }
      case None =>
        throw new UnsupportedDataTypeException(
          s"Failed to find scoring implementation for data type '$typeId'")
    }
  }

  abstract class Scorer {
    def score(data: Record): Option[Double]
  }

  class KnownDrugScorer extends Scorer {
    override def score(data: Record): Option[Double] = {
      val v = data
        .get[Double](FieldName.evidence$drug2clinic$resource_score$value)
        .get *
        data.get[Long](FieldName.evidence$target2drug$resource_score$value).get
      Some(v)
    }
  }

  class AnimalModelScorer extends Scorer {
    override def score(data: Record): Option[Double] = {
      data.get[Double](
        FieldName.evidence$disease_model_association$resource_score$value)
    }
  }

  class RnaExpressionScorer extends Scorer {
    override def score(data: Record): Option[Double] = {
      val pValue = Utilities.scorePValue(
        data.get[Double](FieldName.evidence$resource_score$value).get)
      val log2FoldChange =
        data.get[Double](FieldName.evidence$log2_fold_change$value).get
      val pRank = data
        .get[Long](FieldName.evidence$log2_fold_change$percentile_rank)
        .get / 100.0
      val foldScaleFactor = Math.abs(log2FoldChange) / 10.0
      val score = pValue * foldScaleFactor * pRank
      Some(Math.min(score, 1.0))
    }
  }

  class AffectedPathwayScorer extends Scorer {
    override def score(data: Record): Option[Double] = {
      val score = data.get[Double](FieldName.evidence$resource_score$value).get
      val dataTypeId =
        data.get[String](FieldName.evidence$resource_score$type).get
      dataTypeId match {
        // Convert 0-1 p value score to .5 to 1 scale
        // See: https://github.com/opentargets/data_pipeline/blob/d82f4cfc1e92ab58f1ab5b5553a03742e808d9df/mrtarget/common/EvidenceString.py#L660
        case "pvalue" =>
          Some(.5 * Utilities.scorePValue(score, rng = (1e-14, 1e-4)) + .5)
        case _ => Some(score)
      }
    }
  }

  class LiteratureScorer extends Scorer {
    override def score(data: Record): Option[Double] = {
      val score = data.get[Double](FieldName.evidence$resource_score$value).get
      data.sourceId match {
        case s if s == DataSource.europepmc.toString =>
          Some(Math.min(score / 100.0, 1.0))
        case _ => Some(score)
      }
    }
  }

  class SomaticMutationScorer extends Scorer {

    def processMutation(mutation: Row): Option[(Long, Long)] = {
      if (!mutation.schema.fieldNames.contains(
            "number_samples_with_mutation_type")) {
        return None
      }
      if (mutation.isNullAt(
            mutation.fieldIndex("number_samples_with_mutation_type"))) {
        return None
      }
      if (!mutation.schema.fieldNames.contains("number_mutated_samples")) {
        throw new InvalidRecordException(
          "Record for somatic_mutation source is missing required field 'number_mutated_samples'")
      }
      Some(
        (mutation.getAs[Long]("number_samples_with_mutation_type"),
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
        sampleTotalCoverage = Math.min(sampleTotalCoverage, maxSampleSize)
        frequency = Utilities.normalize(sampleTotalCoverage / maxSampleSize,
                                        (0.0, 9.0),
                                        (0.5, 1.0))
      }
      Some(
        data
          .get[Double](FieldName.evidence$resource_score$value)
          .get * frequency)
    }
  }

  class GeneticAssociationScorer extends Scorer {

    case class PhewasParams(maxCases: Int, rangeMin: Double, rangeMax: Double)
    val PhewasSources: Map[String, PhewasParams] = Map(
      DataSource.phewas_catalog.toString -> PhewasParams(8800, 1e-25, .05),
      DataSource.twentythreeandme.toString -> PhewasParams(297901, 1e-30, .05)
    )

    def scoreNoG2V(data: Record): Option[Double] = {
      val score = data.get[Double](FieldName.evidence$resource_score$value).get
      val resourceScoreType =
        data.get[String](FieldName.evidence$resource_score$type).get
      resourceScoreType match {
        case "probability" => Some(score)
        case "pvalue"      => Some(Utilities.scorePValue(score))
        case _ =>
          throw new UnsupportedResourceScoreTypeException(
            s"Resource score type '$resourceScoreType' not valid")
      }
    }

    def scorePhewas(data: Record): Option[Double] = {
      val pValue = data
        .get[Double](FieldName.evidence$variant2disease$resource_score$value)
        .get
      val nCases =
        data.get[String](FieldName.unique_association_fields$cases).get.toInt
      val params = PhewasSources(data.sourceId)
      val normP =
        Utilities.scorePValue(pValue, rng = (params.rangeMin, params.rangeMax))
      val normN = Utilities.normalize(nCases, (0, params.maxCases), (0, 1))
      Some(normP * normN)
    }

    def scoreGwas(data: Record): Option[Double] = {
      val pValue = data
        .get[Double](FieldName.evidence$variant2disease$resource_score$value)
        .get
      val g2vScore = data
        .get[Double](FieldName.evidence$gene2variant$resource_score$value)
        .get
      val nCases =
        data.get[Long](FieldName.evidence$variant2disease$gwas_sample_size).get
      val normP = Utilities.scorePValue(pValue, rng = (1e-15, 1))
      val normN = Utilities.normalize(nCases, curRng = (0, 5000), (0, 1))
      // There is also an r2 term in the score (for LD presumably) but this field does not exist in the schema
      // See: https://github.com/opentargets/data_pipeline/blob/d82f4cfc1e92ab58f1ab5b5553a03742e808d9df/mrtarget/common/EvidenceString.py#L615
      // val r2 = data.get[Double](FieldName.unique_association_fields$r2).get
      Some(normP * normN * g2vScore)
    }

    def scoreDefault(data: Record): Option[Double] = {
      val g2vScore = data
        .get[Double](FieldName.evidence$gene2variant$resource_score$value)
        .get
      var v2dScore = data
        .get[Double](FieldName.evidence$variant2disease$resource_score$value)
        .get
      val resourceScoreType = data
        .get[String](FieldName.evidence$variant2disease$resource_score$type)
        .get
      v2dScore = resourceScoreType match {
        case "probability" => v2dScore
        case "pvalue"      => Utilities.scorePValue(v2dScore)
        case _ =>
          throw new UnsupportedResourceScoreTypeException(
            s"Resource score type '$resourceScoreType' not valid")
      }
      Some(g2vScore * v2dScore)
    }

    override def score(data: Record): Option[Double] = {
      val g2vExists = data.exists(FieldPath.evidence$gene2variant)
      (g2vExists, data.sourceId) match {
        case (false, _)                                      => scoreNoG2V(data)
        case (_, s) if PhewasSources.contains(s)             => scorePhewas(data)
        case (_, s) if s == DataSource.gwas_catalog.toString => scoreGwas(data)
        case _                                               => scoreDefault(data)
      }
    }
  }

  /**
    * Scoring implementations by data type
    */
  object Scorer extends Enumeration {

    case class Val(name: DataType.Value, scorer: Scorer) extends super.Val

    import scala.language.implicitConversions

    implicit def valueToVal(x: Value): Val = x.asInstanceOf[Val]

    def byTypeId(id: String): Option[Scorer] = {
      Scorer.values.find(_.name.toString == id).map(_.scorer)
    }

    val KnownDrug = Val(DataType.known_drug, new KnownDrugScorer)
    val RnaExpression = Val(DataType.rna_expression, new RnaExpressionScorer)
    val AnimalModel = Val(DataType.animal_model, new AnimalModelScorer)
    val SomaticMutation =
      Val(DataType.somatic_mutation, new SomaticMutationScorer)
    val Literature = Val(DataType.literature, new LiteratureScorer)
    val GeneticAssociation =
      Val(DataType.genetic_association, new GeneticAssociationScorer)
    val AffectedPathwayScorer =
      Val(DataType.affected_pathway, new AffectedPathwayScorer)
  }

}
