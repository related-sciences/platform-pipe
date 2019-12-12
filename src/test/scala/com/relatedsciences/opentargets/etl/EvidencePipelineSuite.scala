package com.relatedsciences.opentargets.etl
import java.nio.file.Paths

import com.relatedsciences.opentargets.etl.configuration.Configuration.Config
import com.relatedsciences.opentargets.etl.pipeline.{EvidencePreparationPipeline, PipelineState}
import org.apache.log4j.Logger
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, length}
import org.scalatest.FunSuite

/**
  * Test for end-to-end evidence pipeline validation
  *
  * Expected results are computed by notebooks in notebooks/testing
  */
class EvidencePipelineSuite extends FunSuite with SparkSessionWrapper with DataFrameComparison {

  import ss.implicits._
  val logger: Logger = Logger.getLogger(getClass.getName)

  test("evidence pipeline results") {
    val config = TestUtils.primaryTestConfig

    // Read in the raw evidence data exported for a few select targets and
    // run the full scoring pipeline on it
    logger.info(s"Beginning evidence pipeline test")
    val state = new PipelineState
    new EvidencePreparationPipeline(ss, config).spec().run(state)
    val refs = state.references.map(v => v.name -> v.value).toMap

    var df = refs("parseEvidenceData").asInstanceOf[Dataset[_]]
    val initialCt = df.count()
    assert(
      df.columns.length > 3,
      s"Evidence json did not parse into Spark schema correctly (columns found = ${df.columns}"
    )
    Seq("disease", "target", "type").foreach { c =>
      assert(df.columns.contains(c), s"Column $c not found in parsed evidence")
    }
    assertResult(0, "Ids should be non-null")(df.filter($"id".isNull).count())
    assertResult(0, "Ids should be 32 char md5")(df.filter(length($"id") =!= 32).count())

    df = refs("normalizeTargetIds").asInstanceOf[Dataset[_]]
    assertResult(initialCt)(df.count)
    val idTypes =
      df.select(df("context.target_id_type")).distinct.map(_(0).toString).collect().toSet
    assertResult(Set("uniprot", "ensembl"))(idTypes)
    assertResult(0L, "Target ids still contain URLs")(df.filter($"target.id".contains("/")).count())
    assertResult(0L, "Target ids are not all ensembl ids")(
      df.filter(!$"target.id".startsWith("ENSG")).count()
    )

    val getIdCt = (ids: Seq[String]) =>
      df.filter($"target.id".isin(ids: _*))
        .select($"target.id")
        .distinct
        .count()

    // There are two non-reference genes in the test data with non-ensembl ids: ENSG00000223532, ENSG00000225845
    // These map to the following ids, respectively, so validate that the mapping
    // occurred: ENSG00000234745, ENSG00000204290
    assertResult(0, "Non-reference alt genes not mapped")(
      getIdCt(Seq("ENSG00000223532", "ENSG00000225845"))
    )
    assertResult(2, "Reference genes not mapped")(
      getIdCt(Seq("ENSG00000234745", "ENSG00000204290"))
    )
    // Similarly, check the expected UniProt ids
    assertResult(0, "UniProt ids still exist")(getIdCt(Seq("P35354", "P10275")))
    assertResult(2, "Expected ids for UniProt ids not found")(
      getIdCt(Seq("ENSG00000073756", "ENSG00000169083"))
    )

    df = refs("normalizeDiseaseIds").asInstanceOf[Dataset[_]]
    assertResult(initialCt)(df.count)
    // Check that other than the one id expected to be bad, all others match the usual (EFO|Orphanet|etc.)_[0-9]+ format
    val invalidDiseaseIds = df
        .filter(col("disease.id") =!= "BAD_EFO_ID")
        .filter(!col("disease.id").rlike("^[a-zA-Z]+_[0-9]+$"))
        .select("disease.id").collect().toList.map(_(0))
    assertResult(0, s"Found invalid disease ids: ${invalidDiseaseIds}")(invalidDiseaseIds.size)

    // Validate that expected bad records are still present
    assertResult(1)(df.filter($"target.id".endsWith("ENSG999999")).count)
    assertResult(1)(df.filter($"disease.id".contains("BAD_EFO_ID")).count)
    assertResult(1)(df.filter($"target.id".contains("ENSG00000240253") && $"sourceID" === "expression_atlas").count)
    assertResult(1)(df.filter($"target.id".contains("ENSG00000169174") && $"sourceID" === "expression_atlas" && $"disease.id" === "EFO_0004267").count)

    df = refs("validateDiseaseIds").asInstanceOf[Dataset[_]]
    // Check that bad records are all gone after validation
    assertResult(0)(df.filter($"target.id".endsWith("ENSG999999")).count)
    assertResult(0)(df.filter($"disease.id".contains("BAD_EFO_ID")).count)
    assertResult(0)(df.filter($"target.id".contains("ENSG00000240253") && $"sourceID" === "expression_atlas").count)
    // Check single fake record with non-filtered biotype
    assertResult(1)(df.filter($"target.id".contains("ENSG00000169174") && $"sourceID" === "expression_atlas" && $"disease.id" === "EFO_0004267").count)
  }

}
