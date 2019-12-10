package com.relatedsciences.opentargets.etl.configuration
import java.net.URL
import java.nio.file.Paths

object Configuration {

  case class PipelineScoring(
      allowUnknownDataType: Boolean,
      allowMissingScore: Boolean,
      saveEvidenceScores: Boolean,
      evidenceExtractDirName: String,
      sourceWeights: Map[String, Double]
  )
  case class PipelineEvidence(
      rawEvidenceDirName: String,
      geneExtractDirName: String
  )
  case class PipelineDecoratorConfig(enabled: Boolean)
  case class ExternalConfig(mrtargetData: URL, mrtargetEs: URL)
  case class Pipeline(scoring: PipelineScoring,
                      evidence: PipelineEvidence,
                      decorators: Map[String, PipelineDecoratorConfig])
  case class Config(
      sparkUri: String,
      inputDir: String,
      outputDir: String,
      resourceDir: String,
      logLevel: String,
      evidenceJsonSchema: String,
      externalConfig: ExternalConfig,
      pipeline: Pipeline
  ) {

    // Evidence prep paths
    lazy val evidenceValidationSummaryPath: String =
      Paths.get(outputDir).resolve("evidence_schema_validation_summary.parquet").toString
    lazy val evidenceValidationErrorsPath: String =
      Paths.get(outputDir).resolve("evidence_schema_validation_errors.parquet").toString

    // Entity data paths
    lazy val geneDataPath: String =
      Paths.get(inputDir).resolve(pipeline.evidence.geneExtractDirName).toString
    lazy val nonRefGeneDataPath: String =
      Paths.get(resourceDir).resolve("genes_with_non_reference_ensembl_ids_lkp.tsv").toString

    // Post evidence-prep
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
      Paths.get(inputDir).resolve(pipeline.evidence.rawEvidenceDirName).toString
    lazy val evidenceExtractPath
      : String = // This contains extracts from ES that should ultimately be used in pipeline
      Paths.get(inputDir).resolve(pipeline.scoring.evidenceExtractDirName).toString

  }

}
