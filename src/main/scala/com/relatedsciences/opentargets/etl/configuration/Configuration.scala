package com.relatedsciences.opentargets.etl.configuration
import java.net.URL
import java.nio.file.Paths

import pureconfig.generic.auto._
import pureconfig._

object Configuration {

  case class PipelineScoring(
      allowUnknownDataType: Boolean,
      allowMissingScore: Boolean,
      saveEvidenceScores: Boolean,
      evidenceFileName: String,
      sourceWeights: Map[String, Double]
  )
  case class PipelineDecoratorConfig(enabled: Boolean)
  case class ExternalConfig(mrtargetData: URL, mrtargetEs: URL)
  case class Pipeline(scoring: PipelineScoring, decorators: Map[String, PipelineDecoratorConfig])
  case class Config(
      sparkUri: String,
      inputDir: String,
      outputDir: String,
      logLevel: String,
      evidenceJsonSchema: String,
      externalConfig: ExternalConfig,
      pipeline: Pipeline
  ) {

    lazy val evidenceValidationSummaryPath: String =
      Paths.get(outputDir).resolve("evidence_validation_summary.parquet").toString
    lazy val preparedEvidencePath: String =
      Paths.get(outputDir).resolve("evidence.parquet").toString
    lazy val evidenceScorePath: String =
      Paths.get(outputDir).resolve("score_evidence.parquet").toString
    lazy val sourceScorePath: String = Paths.get(outputDir).resolve("score_source.parquet").toString
    lazy val associationScorePath: String =
      Paths.get(outputDir).resolve("score_association.parquet").toString

    // Paths that will change in the near future:
    lazy val rawEvidencePath
        : String = // This is a stop-gap destination for downloaded public GS files
      Paths.get(inputDir).resolve("evidence-files").toString
    lazy val evidenceExtractPath
        : String = // This contains extracts from ES that should ultimately be in pipeline
      Paths.get(inputDir).resolve(pipeline.scoring.evidenceFileName).toString

  }

}
