package com.relatedsciences.opentargets.pipeline
import com.relatedsciences.opentargets.pipeline.Utils
import java.util.{Map => JMap}
import java.nio.file.Paths
import org.yaml.snakeyaml.Yaml
import scala.collection.JavaConversions.mapAsScalaMap
import scala.io.Source

object Configuration {

  val INPUT_DIR = Paths.get(System.getProperty("user.home"), "data", "ot", "extract")
  val OUTPUT_DIR = Paths.get(System.getProperty("user.home"), "data", "ot", "results")
  val CONFIG_DIR = Paths.get(System.getProperty("user.home"), "repos", "ot-scoring", "config")

  def loadConfig(path: String): JMap[String, Any] = {
    val content = Utils.using(Source.fromFile(path))(f => f.mkString)
    (new Yaml).load(content).asInstanceOf[JMap[String, Any]]
  }

  def loadScoringConfig(path: String): Map[String, Double] = {
    mapAsScalaMap(
      loadConfig(path)
        .get("scoring_weights").asInstanceOf[JMap[String, Any]]
        .get("source").asInstanceOf[JMap[String, Double]]
    ).toMap
  }
}