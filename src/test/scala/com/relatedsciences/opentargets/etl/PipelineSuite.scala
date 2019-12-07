package com.relatedsciences.opentargets.etl
import java.nio.file.Paths

import com.relatedsciences.opentargets.etl.configuration.Configuration.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.col
import org.scalatest.FunSuite

class PipelineSuite extends FunSuite with SparkSessionWrapper with DataFrameComparison {

  val logger: Logger = Logger.getLogger(getClass.getName)

  def checkScores(
      fields: Seq[String],
      actualPath: String,
      expectedPath: String,
      scoreType: String
  ): Unit = {
    val cols = fields.map(col)
    // Read in actual and expected data with same row and column order
    val dfa = ss.read.parquet(actualPath).select(cols: _*).orderBy(cols: _*)
    val dfe = ss.read.json(expectedPath).select(cols: _*).orderBy(cols: _*)
    logger.info(
      s"Comparing $scoreType scores for ${dfa.count()} actual rows, ${dfe.count()} expected rows"
    )
    assert(!dfa.isEmpty)
    assert(!dfe.isEmpty)
    assertDataFrameEquals(dfa, dfe, tol = epsilon)
  }

  /**
    * Verify that that the "assocation" scores aggregated to the target + disease level are equivalent
    */
  def checkAssocationScores(config: Config): Unit = {
    checkScores(
      Seq("target_id", "disease_id", "score"),
      config.associationScorePath,
      Paths.get(config.inputDir).resolve("association_scores.json").toString,
      "association"
    )
  }

  /**
    * Verify that that the "source" scores aggregated to the target + disease + source level are equivalent
    */
  def checkSourceScores(config: Config): Unit = {
    checkScores(
      Seq("target_id", "disease_id", "source_id", "score"),
      config.sourceScorePath,
      Paths.get(config.inputDir).resolve("source_scores.json").toString,
      "source"
    )
  }

  test("pipeline aggregations are valid for select targets") {
    val config = TestUtils.primaryTestConfig

    // Read in the raw evidence data exported for a few select targets and
    // run the full scoring pipeline on it
    logger.info(s"Beginning full pipeline test")
    Command.CommandEnum.PrepareScores.factory(ss, config).run()
    Command.CommandEnum.CalculateScores.factory(ss, config).run()

    // Check that aggregations to different levels are equivalent to verified values (from OT)
    logger.info(s"Checking association scores")
    checkAssocationScores(config)

    logger.info(s"Checking source scores")
    checkSourceScores(config)
  }

}
