package com.relatedsciences.opentargets.etl

import com.relatedsciences.opentargets.etl.schema.DataType
import com.relatedsciences.opentargets.etl.scoring.{Parameters, Scoring}
import org.scalatest.FunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

class ScoringSuite extends FunSuite with SparkSessionWrapper with DataFrameComparison {

  def assertScores(dataType: DataType.Value): Unit = {
    // Load the test data for the data type (which will contain expected scores)
    val path = getClass.getResource(s"/scorer_test/$dataType.json").getPath
    var df   = ss.read.json(path)

    // Compute the actual scores given just the fields necessary for this source
    val scoreUDF = udf(
      (row: Row) => {
        val record = new Record(
          row.getAs[String]("id"),
          row.getAs[String]("type_id"),
          row.getAs[String]("source_id"),
          row
        )
        val params = Parameters.default()
        // Results from UDFs for scalars are non-nullable unless
        // provided as options, which is necessary in this case
        // for compatibility with the inferred expected score column
        Some(Scoring.score(record, params).get.score)
      },
      DoubleType
    )
    df = df.withColumn("actual_score", scoreUDF(struct(df.columns.map(df(_)): _*)))

    // Create two putatively identical data frames, one with all identifiers and the actual
    // score and another with the same + expected score
    val cols = df.columns.filter(!Seq("actual_score", "expected_score").contains(_)).toList
    val dfExpected = df
      .select(("expected_score" :: cols).map(col): _*)
      .withColumnRenamed("expected_score", "score")
    val dfActual = df
      .select(("actual_score" :: cols).map(col): _*)
      .withColumnRenamed("actual_score", "score")

    // Validate equality, which will print helpful errors
    assertDataFrameEquals(dfExpected, dfActual, tol = epsilon)
  }

  test("known_drug score calculations") {
    assertScores(DataType.known_drug)
  }

  test("animal_model score calculations") {
    assertScores(DataType.animal_model)
  }

  test("affected_pathway score calculations") {
    assertScores(DataType.affected_pathway)
  }

  test("genetic_association score calculations") {
    assertScores(DataType.genetic_association)
  }

  test("rna_expression score calculations") {
    assertScores(DataType.rna_expression)
  }

  test("somatic_mutation score calculations") {
    assertScores(DataType.somatic_mutation)
  }

}
