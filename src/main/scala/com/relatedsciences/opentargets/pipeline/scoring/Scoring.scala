package com.relatedsciences.opentargets.pipeline.scoring

import com.relatedsciences.opentargets.pipeline.schema.Fields.{FieldName, FieldPath}
import com.relatedsciences.opentargets.pipeline.schema.{DataSource, DataType}
import com.relatedsciences.opentargets.pipeline.scoring.Component.ComponentName
import com.relatedsciences.opentargets.pipeline.{Record, Utilities}
import org.apache.spark.sql.Row

object Scoring {

  class UnsupportedDataTypeException(message: String)          extends Exception(message)
  class UnsupportedResourceScoreTypeException(message: String) extends Exception(message)
  class InvalidRecordException(message: String) extends Exception(message) {
    def this(message: String, cause: Throwable) {
      this(message)
      initCause(cause)
    }
  }

  /**
    * Compute the association score for a single evidence string record
    *
    * @param data record object containing evidence data as well as provenance (data source, type, etc.)
    * @param params parameters to be used in scoring (e.g. field weights)
    * @return Option[Score]
    */
  def score(data: Record, params: Parameters): Option[Score] = {
    Scorer.byTypeId(data.typeId) match {
      case Some(scorer) =>
        try {
          scorer.score(data, params)
        } catch {
          case e: Exception => throw new InvalidRecordException(s"Failed record: $data", e)
        }
      case None =>
        throw new UnsupportedDataTypeException(
          s"Failed to find scoring implementation for data type '${data.typeId}'"
        )
    }
  }

  abstract class Scorer {
    def score(data: Record, params: Parameters): Option[Score]
  }

  class KnownDrugScorer extends Scorer {
    override def score(data: Record, params: Parameters): Option[Score] = {
      Score
        .using(params.weights)
        .add(
          ComponentName.drug2clinic,
          data.get[Double](FieldName.evidence$drug2clinic$resource_score$value).get
        )
        .add(
          ComponentName.target2drug,
          data.get[Long](FieldName.evidence$target2drug$resource_score$value).get
        )
        .calculate()
    }
  }

  class AnimalModelScorer extends Scorer {
    override def score(data: Record, params: Parameters): Option[Score] = {
      Score
        .using(params.weights)
        .add(
          ComponentName.disease_model_association,
          data.get[Double](FieldName.evidence$disease_model_association$resource_score$value).get
        )
        .calculate()
    }
  }

  class RnaExpressionScorer extends Scorer {
    override def score(data: Record, params: Parameters): Option[Score] = {
      val pValue =
        Utilities.scorePValue(data.get[Double](FieldName.evidence$resource_score$value).get)
      val log2FoldChange =
        data.get[Double](FieldName.evidence$log2_fold_change$value).get
      val pRank = data
        .get[Long](FieldName.evidence$log2_fold_change$percentile_rank)
        .get / 100.0
      val foldScaleFactor = Math.abs(log2FoldChange) / 10.0
      Score
        .using(params.weights)
        .add(ComponentName.p_value, pValue)
        .add(ComponentName.log2_fold_scale_factor, foldScaleFactor)
        .add(ComponentName.log2_fold_change_rank, pRank)
        .calculate(Math.min(_, 1.0))
    }
  }

  class AffectedPathwayScorer extends Scorer {
    override def score(data: Record, params: Parameters): Option[Score] = {
      var score = data.get[Double](FieldName.evidence$resource_score$value).get
      val dataTypeId =
        data.get[String](FieldName.evidence$resource_score$type).get
      if (dataTypeId == "pvalue") {
        // Convert 0-1 p value score to .5 to 1 scale
        // See: https://github.com/opentargets/data_pipeline/blob/d82f4cfc1e92ab58f1ab5b5553a03742e808d9df/mrtarget/common/EvidenceString.py#L660
        score = .5 * Utilities.scorePValue(score, rng = (1e-14, 1e-4)) + .5
      }
      Score
        .using(params.weights)
        .add(ComponentName.resource_score, score)
        .calculate()
    }
  }

  class LiteratureScorer extends Scorer {
    override def score(data: Record, params: Parameters): Option[Score] = {
      var score = data.get[Double](FieldName.evidence$resource_score$value).get
      if (data.sourceId == DataSource.europepmc.toString) {
        score = Math.min(score / 100.0, 1.0)
      }
      Score
        .using(params.weights)
        .add(ComponentName.resource_score, score)
        .calculate()
    }
  }

  class SomaticMutationScorer extends Scorer {

    def processMutation(mutation: Row): Option[(Long, Long)] = {
      if (!mutation.schema.fieldNames.contains("number_samples_with_mutation_type")) {
        return None
      }
      if (mutation.isNullAt(mutation.fieldIndex("number_samples_with_mutation_type"))) {
        return None
      }
      if (!mutation.schema.fieldNames.contains("number_mutated_samples")) {
        throw new InvalidRecordException(
          "Record for somatic_mutation source is missing required field 'number_mutated_samples'"
        )
      }
      Some(
        (
          mutation.getAs[Long]("number_samples_with_mutation_type"),
          mutation.getAs[Long]("number_mutated_samples")
        )
      )
    }

    override def score(data: Record, params: Parameters): Option[Score] = {
      var frequency = 1.0
      val mutations = data.get[Seq[Row]](FieldName.evidence$known_mutations)
      if (mutations.isDefined && mutations.get.nonEmpty) {
        var sampleTotalCoverage = 1.0
        var maxSampleSize       = 1.0
        for (t <- mutations.get.flatMap(processMutation)) {
          sampleTotalCoverage += t._1
          maxSampleSize = Math.max(maxSampleSize, t._2)
        }
        sampleTotalCoverage = Math.min(sampleTotalCoverage, maxSampleSize)
        frequency = Utilities.normalize(sampleTotalCoverage / maxSampleSize, (0.0, 9.0), (0.5, 1.0))
      }
      val score = data.get[Double](FieldName.evidence$resource_score$value).get
      Score
        .using(params.weights)
        .add(ComponentName.resource_score, score)
        .add(ComponentName.mutation_frequency, frequency)
        .calculate()
    }
  }

  class GeneticAssociationScorer extends Scorer {

    case class PhewasParams(maxCases: Int, rangeMin: Double, rangeMax: Double)
    val PhewasSources: Map[String, PhewasParams] = Map(
      DataSource.phewas_catalog.toString   -> PhewasParams(8800, 1e-25, .05),
      DataSource.twentythreeandme.toString -> PhewasParams(297901, 1e-30, .05)
    )

    def scoreNoG2V(data: Record, params: Parameters): Option[Score] = {
      var score = data.get[Double](FieldName.evidence$resource_score$value).get
      val resourceScoreType =
        data.get[String](FieldName.evidence$resource_score$type).get
      score = resourceScoreType match {
        case "probability" => score
        case "pvalue"      => Utilities.scorePValue(score)
        case _ =>
          throw new UnsupportedResourceScoreTypeException(
            s"Resource score type '$resourceScoreType' not valid"
          )
      }
      Score
        .using(params.weights)
        .add(ComponentName.resource_score, score)
        .calculate()
    }

    def scorePhewas(data: Record, params: Parameters): Option[Score] = {
      val pValue = data
        .get[Double](FieldName.evidence$variant2disease$resource_score$value)
        .get
      val nCases =
        data.get[String](FieldName.unique_association_fields$cases).get.toInt
      val args = PhewasSources(data.sourceId)
      val normP =
        Utilities.scorePValue(pValue, rng = (args.rangeMin, args.rangeMax))
      val normN = Utilities.normalize(nCases, (0, args.maxCases), (0, 1))
      Score
        .using(params.weights)
        .add(ComponentName.v2d_score, normP)
        .add(ComponentName.sample_size, normN)
        .calculate()
    }

    def scoreGwas(data: Record, params: Parameters): Option[Score] = {
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
      Score
        .using(params.weights)
        .add(ComponentName.v2d_score, normP)
        .add(ComponentName.g2v_score, g2vScore)
        .add(ComponentName.sample_size, normN)
        .calculate()
    }

    def scoreDefault(data: Record, params: Parameters): Option[Score] = {
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
            s"Resource score type '$resourceScoreType' not valid"
          )
      }
      Score
        .using(params.weights)
        .add(ComponentName.v2d_score, v2dScore)
        .add(ComponentName.g2v_score, g2vScore)
        .calculate()
    }

    override def score(data: Record, params: Parameters): Option[Score] = {
      val g2vExists = data.exists(FieldPath.evidence$gene2variant)
      (g2vExists, data.sourceId) match {
        case (false, _)                                      => scoreNoG2V(data, params)
        case (_, s) if PhewasSources.contains(s)             => scorePhewas(data, params)
        case (_, s) if s == DataSource.gwas_catalog.toString => scoreGwas(data, params)
        case _                                               => scoreDefault(data, params)
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

    val KnownDrug     = Val(DataType.known_drug, new KnownDrugScorer)
    val RnaExpression = Val(DataType.rna_expression, new RnaExpressionScorer)
    val AnimalModel   = Val(DataType.animal_model, new AnimalModelScorer)
    val SomaticMutation =
      Val(DataType.somatic_mutation, new SomaticMutationScorer)
    val Literature = Val(DataType.literature, new LiteratureScorer)
    val GeneticAssociation =
      Val(DataType.genetic_association, new GeneticAssociationScorer)
    val AffectedPathwayScorer =
      Val(DataType.affected_pathway, new AffectedPathwayScorer)
  }

}
