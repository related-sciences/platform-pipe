package com.relatedsciences.opentargets.etl
import java.nio.file.{Path, Paths}

import com.relatedsciences.opentargets.etl.configuration.Configuration
import pureconfig.ConfigSource
// Do not remove any of these -- while flagged as unused in IntelliJ, they are necessary for config parsing
import pureconfig._
import pureconfig.generic.auto._
import pureconfig.module.enumeratum._

object TestUtils {

  val TEST_RESOURCE_DIR = Paths.get("src", "test", "resources")

  def getConfigSource(path: String): ConfigObjectSource = {
    ConfigSource
      .resources(path)
      .recoverWith({ case _ => ConfigSource.resources(path.replaceFirst("/", "")) })
  }

  def getConfig(path: String = "/config/application.conf"): Configuration.Config = {
    getConfigSource(path).loadOrThrow[Configuration.Config]
  }

  /** Note that this configuration will NOT come from src/test/resources with the ConfigSource.default loader */
  lazy val primaryTestConfig: Configuration.Config = getConfig()

  lazy val pipelineTestPath: Path =
    Paths.get(getClass.getResource("pipeline_test/README.md").getPath).getParent

}
