package com.relatedsciences.opentargets.etl

import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.{Column, Dataset, Row}
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

case class Organ(name: String, count: Int)
case class Disease(id: Int, name: String, organ: Organ)
case class Drug(id: Int, name: String, alt: Array[String])
case class DataRecord(id: Int, drug: Drug, disease: Disease)

case class RecL2(v1: Option[List[Int]], v2: Option[String])
case class RecL1(v1: Option[List[Int]], v2: Option[RecL2])
case class RecL0(id: Int, name: Option[String], s1: Option[RecL1])

class DataFrameOpsSuite extends FunSuite with SparkSessionWrapper with DataFrameComparison {
  import com.relatedsciences.opentargets.etl.pipeline.SparkImplicits._
  import ss.implicits._

  test("struct packing") {
    val df = Seq(
      (1, "x", 1.0, List("a", "b")),
      (2, "y", 2.0, List("x", "y"))
    ).toDF("id", "f1", "f2", "f3")
      .withColumn("f4", struct(
        lit(1).as("if1"),
        lit("a").as("if2")
      ))
    //+---+---+---+------+------+
    //| id| f1| f2|    f3|    f4|
    //+---+---+---+------+------+
    //|  1|  x|1.0|[a, b]|[1, a]|
    //|  2|  y|2.0|[x, y]|[1, a]|
    //+---+---+---+------+------+
    //root
    //|-- id: integer (nullable = false)
    //|-- f1: string (nullable = true)
    //|-- f2: double (nullable = false)
    //|-- f3: array (nullable = true)
    //|    |-- element: string (containsNull = true)
    //|-- f4: struct (nullable = false)
    //|    |-- if1: integer (nullable = false)
    //|    |-- if2: string (nullable = false)

    // Test creation of non-existent struct
    val df2 = df.appendStruct("obj", "f1", "f2")
    assertResult(Array("id", "f3", "f4", "obj"))(df2.columns)
    assertResult(Array("f1", "f2"))(df2.select("obj.*").columns)
    assertResult(List(List("x", 1.0), List("y", 2.0)))(
      df2.select("obj.*").rdd.collect().toList.map(_.toSeq.toList)
    )

    // Test single column append
    val df3 = df2.appendStruct("obj", "f3")
    assertResult(Array("id", "f4", "obj"))(df3.columns)
    assertResult(Array("f1", "f2", "f3"))(df3.select("obj.*").columns)

    // Test multi-column append
    val df4 = df2.appendStruct("obj", "f3", "f4")
    assertResult(Array("id", "obj"))(df4.columns)
    assertResult(Array("f1", "f2", "f3", "f4"))(df4.select("obj.*").columns)
  }

  test("inner struct nullability") {
    // This should ensure that nested structs remain null following recursive manipulation, if already null
    val df = Seq(
      RecL0(1, Some("x"), Some(RecL1(Some(List(1, 2)), Some(RecL2(Some(List(1,2)), Some("x")))))),
      RecL0(2, Some("y"), Some(RecL1(Some(List(1, 2)), None))),
      RecL0(3, None, None)
    ).toDF
    //df.printSchema
    //root
    //|-- id: integer (nullable = false)
    //|-- name: string (nullable = true)
    //|-- s1: struct (nullable = true)
    //|    |-- v1: array (nullable = true)
    //|    |    |-- element: integer (containsNull = false)
    //|    |-- v2: struct (nullable = true)
    //|    |    |-- v1: array (nullable = true)
    //|    |    |    |-- element: integer (containsNull = false)
    //|    |    |-- v2: string (nullable = true)

    val dfm = df.mutate(PartialFunction.empty)
    assertDataFrameEquals(dfm, df)
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
      assertResult(expected.schema)(actual.schema)
      assertResult(expected.count())(actual.count())
    }

    val fn1: PartialFunction[String, Column] = { case "disease.organ.count" => $"disease.organ.count" - 1 }
    implicit val encoder: ExpressionEncoder[Row] = RowEncoder(df1.schema)

    // Validate mutation through implicits and underlying static method
    val df2 = df1.mutate(fn1)
    validate(df1, df2)
    validate(df1, df1.mutateColumns("disease.organ.count")(c => col(c) - 1)) // Validate overload too
    validate(df1, Utilities.applyToDataset(df1, fn1))
    assertResult(df1.as[DataRecord].map(r => r.disease.organ.count).rdd.collect())(
      df2.as[DataRecord].map(r => r.disease.organ.count + 1).rdd.collect()
    )

    // Validate mutation on top-level field
    val fn2: PartialFunction[String, Column] = { case "id" => $"id" * 2 }
    val df3                   = df1.mutate(fn2)
    validate(df1, df3)
    validate(df1, Utilities.applyToDataset(df1, fn2))
    assertResult(df1.as[DataRecord].map(r => r.id).rdd.collect())(
      df3.as[DataRecord].map(r => r.id / 2).rdd.collect()
    )

    // Validate mutation with typed dataset
    val ds1 = df1.as[DataRecord]
    val ds2 = Utilities.applyToDataset(ds1, fn1)
    validate(ds1, ds2)
    assertResult(ds1.as[DataRecord].map(r => r.disease.organ.count).rdd.collect())(
      ds2.as[DataRecord].map(r => r.disease.organ.count + 1).rdd.collect()
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

    // Test noop mutation for schema and value equality
    assertDataFrameEquals(df1.mutate(PartialFunction.empty), df1)

    // For both of the above records, the value at this path is an array<struct<lit_id: string>>
    // with the value [<lit_id: "http://europepmc.org/abstract/MED/28179366"] (only one element).
    // Mutate that value to only contain the last part of the url:
    val df2 = df1.mutate({
      case "evidence.provenance_type.literature.references" => array(struct(element_at(
        split(element_at($"evidence.provenance_type.literature.references", 1)("lit_id"), "/"),
        -1)))
    })
    val actual = df2
      .select($"evidence.provenance_type.literature.references".getItem(0).getField("lit_id"))
      .collect()(0)(0)
    assertResult("28179366")(actual)
    assertResult(df1.schema)(df2.schema)

    // In the second record, disease.id was removed manually so test that mutations at that level work with nullable values
    val df3 = df1.mutate({
      case "disease.id" => element_at(split($"disease.id", "/"), -1)
    })
    val actual2 = df3.select($"disease.id").collect().map(_(0)).toSeq
    assertResult(Seq("EFO_1000233", null))(actual2)
  }

}
