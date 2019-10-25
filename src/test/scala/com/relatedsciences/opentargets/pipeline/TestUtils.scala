package com.relatedsciences.opentargets.pipeline
import java.nio.file.Paths

object TestUtils {

  def getPipelineConfig(): Configuration = {
    val path = Paths.get(getClass.getResource("/pipeline_test/config/scoring.yml").getPath).getParent
    Configuration(
      inputDir=path.getParent.resolve("input"),
      outputDir=path.getParent.resolve("output"),
      configDir=path
    )
  }

}
