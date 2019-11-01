package com.relatedsciences.opentargets.pipeline
import java.nio.file.Paths

object TestUtils {

  def getPipelineConfig(): Configuration = {
    val path =
      Paths.get(getClass.getResource("/pipeline_test/config/scoring.yml").getPath).getParent
    Configuration(
      inputDir = path.getParent.resolve("input").toString,
      outputDir = path.getParent.resolve("output").toString,
      configDir = path.toString,
      evidenceFileName = "evidence.json.gz",
      allowUnknownDataType = true,
      saveEvidenceScores = true
    )
  }

}
