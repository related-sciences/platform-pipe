package com.relatedsciences.opentargets.etl
import java.nio.file.{Path, Paths}

import com.relatedsciences.opentargets.etl.configuration.Configuration
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import pureconfig._

object TestUtils {

  def getConfig(path: String): Configuration.Config = {
    ConfigSource.resources(path).loadOrThrow[Configuration.Config]
  }

  /** Note that this configuration will NOT come from src/test/resources with the ConfigSource.default loader */
  lazy val primaryTestConfig: Configuration.Config = getConfig("config/application.conf")

  lazy val pipelineTestPath: Path =
    Paths.get(getClass.getResource("/pipeline_test/README.md").getPath).getParent

  def getPipelineConfig(): ConfigurationOld = {
    val path =
      Paths.get(getClass.getResource("/pipeline_test/config/scoring.yml").getPath).getParent
    ConfigurationOld(
      inputDir = path.getParent.resolve("input").toString,
      outputDir = path.getParent.resolve("output").toString,
      configDir = path.toString,
      evidenceFileName = "evidence.json.gz",
      allowUnknownDataType = true,
      saveEvidenceScores = true
    )
  }

}
