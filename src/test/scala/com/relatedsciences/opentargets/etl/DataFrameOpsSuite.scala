package com.relatedsciences.opentargets.etl

import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.{Column, Dataset, Row}
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

case class Organ(name: String, count: Int)
case class Disease(id: Int, name: String, organ: Organ)
case class Drug(id: Int, name: String, alt: Array[String])
case class Record(id: Int, drug: Drug, disease: Disease)

class DataFrameOpsSuite extends FunSuite with SparkSessionWrapper {
  import com.relatedsciences.opentargets.etl.pipeline.SparkImplicits._
  import ss.implicits._

  test("struct packing") {
    val df = Seq(
      (1, "x", 1.0, "a"),
      (2, "y", 2.0, "b")
    ).toDF("id", "f1", "f2", "f3")
    val df2 = df.withStruct("obj", "f2", "f3")
    assertResult(Array("id", "f1", "obj"))(df2.columns)
    assertResult(List(List(1.0, "a"), List(2.0, "b")))(
      df2.select("obj.*").rdd.collect().toList.map(_.toSeq.toList)
    )
  }

  test("basic nested struct mutation") {
    val rows = Seq(
      (1, Drug(1, "drug1", Array("x", "y")), Disease(1, "disease1", Organ("heart", 2))),
      (
        2,
        Drug(2, "drug2", Array("a")),
        Disease(2, "disease2", Organ("eye", 3))
      )
    )
    val df1 = rows.toDF("id", "drug", "disease")

    def validate[T](actual: Dataset[T], expected: Dataset[T]) = {
      assertResult(expected.schema.toString)(actual.schema.toString)
      assertResult(expected.count())(actual.count())
    }

    val fn1: Column => Column                    = c => if (c.toString == "disease.organ.count") c - 1 else c
    implicit val encoder: ExpressionEncoder[Row] = RowEncoder(df1.schema)

    // Validate mutation through implicits and underlying static method
    val df2 = df1.mutate(fn1)
    validate(df1, df2)
    validate(df1, df1.mutateColumns("disease.organ.count")(c => c - 1)) // Validate overload too
    validate(df1, Utilities.applyToDataset(df1, fn1))
    assertResult(df1.as[Record].map(r => r.disease.organ.count).rdd.collect())(
      df2.as[Record].map(r => r.disease.organ.count + 1).rdd.collect()
    )

    // Validate mutation on top-level field
    val fn2: Column => Column = c => if (c.toString == "id") c * 2 else c
    val df3                   = df1.mutate(fn2)
    validate(df1, df3)
    validate(df1, Utilities.applyToDataset(df1, fn2))
    assertResult(df1.as[Record].map(r => r.id).rdd.collect())(
      df3.as[Record].map(r => r.id / 2).rdd.collect()
    )

    // Validate mutation with typed dataset
    val ds1 = df1.as[Record]
    val ds2 = Utilities.applyToDataset(ds1, fn1)
    validate(ds1, ds2)
    assertResult(ds1.as[Record].map(r => r.disease.organ.count).rdd.collect())(
      ds2.as[Record].map(r => r.disease.organ.count + 1).rdd.collect()
    )
  }

  test("advanced nested struct mutation") {
    // Load example evidence record objects
    val json =
      """
        |{"access_level":"public","disease":{"id":"http://www.ebi.ac.uk/efo/EFO_1000233","name":"endometrial endometrioid adenocarcinoma"},"evidence":{"date_asserted":"2018-11-29T12:27:21.042881","evidence_codes":["http://purl.obolibrary.org/obo/ECO_0000053"],"is_associated":true,"provenance_type":{"database":{"dbxref":{"id":"SLAPEnrich analysis of TCGA tumor types","url":"https://saezlab.github.io/SLAPenrich/","version":"2017.08"},"id":"SLAPEnrich","version":"2017.08"},"literature":{"references":[{"lit_id":"http://europepmc.org/abstract/MED/28179366"}]}},"resource_score":{"method":{"description":"SLAPEnrich analysis of TCGA tumor types as described in Brammeld J et al (2017)","reference":"http://europepmc.org/abstract/MED/28179366","url":"https://saezlab.github.io/SLAPenrich"},"type":"pvalue","value":1.08E-15},"urls":[{"nice_name":" Interleukin-4 and 13 signaling","url":"http://www.reactome.org/PathwayBrowser/#R-HSA-168256"}]},"sourceID":"slapenrich","target":{"activity":"http://identifiers.org/cttv.activity/unknown","id":"http://identifiers.org/ensembl/ENSG00000105397","target_name":"TYK2","target_type":"http://identifiers.org/cttv.target/gene_evidence"},"type":"affected_pathway","unique_association_fields":{"efo_id":"http://www.ebi.ac.uk/efo/EFO_1000233","pathway_id":"http://www.reactome.org/PathwayBrowser/#R-HSA-168256","symbol":"TYK2","tumor_type":"endometrial endometrioid adenocarcinoma","tumor_type_acronym":"UCEC"},"validated_against_schema_version":"1.2.8","rid":1}
        |{"access_level":"public","disease":{"name":"brain glioma"},"evidence":{"date_asserted":"2018-11-29T12:27:21.042881","evidence_codes":["http://purl.obolibrary.org/obo/ECO_0000053"],"is_associated":true,"provenance_type":{"database":{"dbxref":{"id":"SLAPEnrich analysis of TCGA tumor types","url":"https://saezlab.github.io/SLAPenrich/","version":"2017.08"},"id":"SLAPEnrich","version":"2017.08"},"literature":{"references":[{"lit_id":"http://europepmc.org/abstract/MED/28179366"}]}},"resource_score":{"method":{"description":"SLAPEnrich analysis of TCGA tumor types as described in Brammeld J et al (2017)","reference":"http://europepmc.org/abstract/MED/28179366","url":"https://saezlab.github.io/SLAPenrich"},"type":"pvalue","value":9.77E-16},"urls":[{"nice_name":" Interleukin-4 and 13 signaling","url":"http://www.reactome.org/PathwayBrowser/#R-HSA-168256"}]},"sourceID":"slapenrich","target":{"activity":"http://identifiers.org/cttv.activity/unknown","id":"http://identifiers.org/ensembl/ENSG00000105397","target_name":"TYK2","target_type":"http://identifiers.org/cttv.target/gene_evidence"},"type":"affected_pathway","unique_association_fields":{"efo_id":"http://www.ebi.ac.uk/efo/EFO_0005543","pathway_id":"http://www.reactome.org/PathwayBrowser/#R-HSA-168256","symbol":"TYK2","tumor_type":"lower grade glioma","tumor_type_acronym":"LGG"},"validated_against_schema_version":"1.2.8","rid":2}
        |""".stripMargin
    val df1 = ss.read.json(json.split("\n").toSeq.toDS)
    assertResult(2L)(df1.count())

    // For both of the above records, the value at this path is an array<struct<lit_id: string>>
    // with the value [<lit_id: "http://europepmc.org/abstract/MED/28179366"] (only one element).
    // Mutate that value to only contain the last part of the url:
    val df2 = df1.mutate(
      c =>
        if (c.toString == "evidence.provenance_type.literature.references")
          array(struct(element_at(split(element_at(c, 1)("lit_id"), "/"), -1)))
        else c
    )
    val actual = df2
      .select($"evidence.provenance_type.literature.references".getItem(0).getField("lit_id"))
      .collect()(0)(0)
    assertResult("28179366")(actual)
    assertResult(df1.schema.toString)(df2.schema.toString)

    // In the second record, disease.id was removed manually so test that mutations at that level work with nullable values
    val df3 = df1.mutate(
      c =>
        if (c.toString == "disease.id") element_at(split(c, "/"), -1)
        else c
    )
    val actual2 = df3.select($"disease.id").collect().map(_(0)).toSeq
    assertResult(Seq("EFO_1000233", null))(actual2)
  }

}
