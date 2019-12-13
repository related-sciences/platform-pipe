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
                               geneExtractDirName: String,
                               efoExtractDirName: String,
                               excludedBiotypes: Map[String, List[String]]
                             )

  case class PipelineDecoratorConfig(enabled: Boolean)

  case class ExternalConfig(mrtargetData: URL, mrtargetEs: URL)

  case class DataSource(name: String, `type`: String)

  case class DataResources(localDir: String, evidenceJsonSchema: String, ecoScores: String)

  case class Pipeline(
                       scoring: PipelineScoring,
                       evidence: PipelineEvidence,
                       decorators: Map[String, PipelineDecoratorConfig],
                       enableAssertions: Boolean,
                       enableSummaries: Boolean
                     )

  case class Config(
                     sparkUri: String,
                     inputDir: String,
                     outputDir: String,
                     logLevel: String,
                     dataSources: List[DataSource],
                     dataResources: DataResources,
                     externalConfig: ExternalConfig,
                     pipeline: Pipeline
                   ) {

    // Evidence prep paths
    lazy val evidenceSchemaValidationSummaryPath: String =
      Paths.get(outputDir).resolve("errors/evidence_schema_validation_summary.parquet").toString
    lazy val evidenceSchemaValidationErrorsPath: String =
      Paths.get(outputDir).resolve("errors/evidence_schema_validation_errors.parquet").toString
    lazy val evidenceTargetIdValidationSummaryPath: String =
      Paths.get(outputDir).resolve("errors/evidence_target_id_validation_summary.parquet").toString
    lazy val evidenceTargetIdValidationErrorsPath: String =
      Paths.get(outputDir).resolve("errors/evidence_target_id_validation_errors.parquet").toString
    lazy val evidenceDiseaseIdValidationSummaryPath: String =
      Paths.get(outputDir).resolve("errors/evidence_disease_id_validation_summary.parquet").toString
    lazy val evidenceDiseaseIdValidationErrorsPath: String =
      Paths.get(outputDir).resolve("errors/evidence_disease_id_validation_errors.parquet").toString
    lazy val evidenceDataSourceValidationSummaryPath: String =
      Paths.get(outputDir).resolve("errors/evidence_data_source_validation_summary.parquet").toString
    lazy val evidenceDataSourceValidationErrorsPath: String =
      Paths.get(outputDir).resolve("errors/evidence_data_source_validation_errors.parquet").toString
    lazy val preparedRawEvidencePath: String =
      Paths.get(outputDir).resolve("evidence_raw.parquet").toString

    // Entity data paths
    lazy val geneDataPath: String =
      Paths.get(inputDir).resolve(pipeline.evidence.geneExtractDirName).toString
    lazy val efoDataPath: String =
      Paths.get(inputDir).resolve(pipeline.evidence.efoExtractDirName).toString
    lazy val nonRefGeneDataPath: String =
      Paths.get(dataResources.localDir).resolve("genes_with_non_reference_ensembl_ids_lkp.tsv").toString

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

    lazy val dataSourceToType: Map[String, String] = dataSources.map(s => s.name -> s.`type`).toMap

  }

}
