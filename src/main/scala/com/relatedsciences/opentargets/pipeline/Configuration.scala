package com.relatedsciences.opentargets.pipeline
import java.nio.file.{Path, Paths}
import java.util.{Map => JMap}

import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConversions.mapAsScalaMap
import scala.io.Source

case class Configuration(
    inputDir: Path,
    outputDir: Path,
    configDir: Path,
    allowUnknownDataType: Boolean = true,
    allowMissingScore: Boolean = false,
    saveEvidenceScores: Boolean = false
) {

  private def loadConfig(path: String): JMap[String, Any] = {
    val content = Utilities.using(Source.fromFile(path))(f => f.mkString)
    (new Yaml).load(content).asInstanceOf[JMap[String, Any]]
  }

  def getScoringConfig(): Map[String, Double] = {
    mapAsScalaMap(
      loadConfig(
        configDir.resolve(Configuration.OT_DATA_CONFIG_FILENAME).toString
      ).get("scoring_weights")
        .asInstanceOf[JMap[String, Any]]
        .get("source")
        .asInstanceOf[JMap[String, Double]]
    ).toMap
  }

}

object Configuration {
  val OT_DATA_CONFIG_FILENAME = "scoring.yml"

  def default(): Configuration = {
    Configuration(
      inputDir = Paths.get(System.getProperty("user.home"), "data", "ot", "extract"),
      outputDir = Paths.get(System.getProperty("user.home"), "data", "ot", "results"),
      configDir = Paths
        .get(System.getProperty("user.home"), "repos", "ot-scoring", "config")
    )
  }
}
