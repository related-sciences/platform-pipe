package com.relatedsciences.opentargets.etl

import java.net.URL

import com.relatedsciences.opentargets.etl.pipeline.JsonValidation.{
  DateValidator,
  RecordValidator,
  URIFormatValidator
}
import org.scalatest.FunSuite

class JsonValidatonSuite extends FunSuite {

  test("custom date format validator") {
    val validator = new DateValidator()
    // Test valid dates
    Seq(
      "2019-10-30T11:55:46.575602",
      "2019-09-25T02:12:12+00:00",
      "2018-11-19T00:00:00.000Z",
      "2018-11-19T00:00:00.000",
      "2019-03-20T16:43:07Z",
      "2019-10-30T11:55:46",
      "2019-10-30"
    ).foreach(
      d =>
        assert(
          !validator.validate(d).isPresent,
          s"Date '$d' expected to be valid but was found invalid"
        )
    )

    // Test invalid dates
    Seq(
      "2019-10-30T",
      "2019-10-30T11",
      "2019-10-30T11:55:4",
      "2019-10-30 11:55:00" // While common this is not a valid ISO 8601 date (https://en.wikipedia.org/wiki/ISO_8601)
    ).foreach(
      d =>
        assert(
          validator.validate(d).isPresent,
          s"Date '$d' expected to be invalid but was found valid"
        )
    )
  }

  test("custom uri validator") {
    val validator = new URIFormatValidator()
    // Test valid uris
    Seq(
      "https://somesite.com/file.pdf",
      "/file.pdf",
      "file.pdf"
    ).foreach(
      d =>
        assert(
          !validator.validate(d).isPresent,
          s"URI '$d' expected to be valid but was found invalid"
        )
    )
    // Test invalid uris
    Seq("").foreach(
      d =>
        assert(
          validator.validate(d).isPresent,
          s"URI '$d' expected to be invalid but was found valid"
        )
    )
  }

  test("evidence record validation") {
    val url = TestUtils.primaryTestConfig.evidenceJsonSchema
    RecordValidator.url = Some(new URL(url))

    // Assign single evidence json record to test
    val goodJson =
      """
        |{"access_level":"public","disease":{"id":"http://www.ebi.ac.uk/efo/EFO_0001359"},"evidence":{"gene2variant":{"date_asserted":"2019-09-25T02:26:58+00:00","evidence_codes":["http://purl.obolibrary.org/obo/ECO_0000205","http://identifiers.org/eco/cttv_mapping_pipeline"],"functional_consequence":"http://purl.obolibrary.org/obo/SO_0001583","is_associated":true,"provenance_type":{"database":{"dbxref":{"id":"http://identifiers.org/gwas_catalog","version":"2019-09-25T02:26:58+00:00"},"id":"GWAS Catalog","version":"2019-09-25T02:26:58+00:00"},"expert":{"statement":"Primary submitter of data","status":true}}},"variant2disease":{"confidence_interval":"1.11-1.22","date_asserted":"2019-09-25T02:26:58+00:00","evidence_codes":["http://identifiers.org/eco/GWAS","http://purl.obolibrary.org/obo/ECO_0000205"],"gwas_panel_resolution":2600000,"gwas_sample_size":29835,"is_associated":true,"odds_ratio":"1.16","provenance_type":{"database":{"dbxref":{"id":"http://identifiers.org/gwas_catalog","version":"2019-09-25T02:26:58+00:00"},"id":"GWAS Catalog","version":"2019-09-25T02:26:58+00:00"},"expert":{"statement":"Primary submitter of data","status":true},"literature":{"references":[{"lit_id":"http://europepmc.org/abstract/MED/19966805"}]}},"resource_score":{"exponent":-9,"mantissa":4,"method":{"description":"pvalue for the snp to disease association."},"type":"pvalue","value":4.0E-9},"unique_experiment_reference":"http://europepmc.org/abstract/MED/19966805"}},"literature":{"references":[{"lit_id":"http://europepmc.org/abstract/MED/19966805"}]},"sourceID":"gwas_catalog",
        |"target":{"activity":"http://identifiers.org/cttv.activity/predicted_damaging","id":"http://identifiers.org/ensembl/ENSG00000105397","target_type":"http://identifiers.org/cttv.target/gene_evidence"},"type":"genetic_association","unique_association_fields":{"disease_id":"http://www.ebi.ac.uk/efo/EFO_0001359","pubmed_refs":"http://europepmc.org/abstract/MED/19966805","pvalue_annotation":"","study_name":"GCST000539","target":"http://identifiers.org/ensembl/ENSG00000105397","variant":"http://identifiers.org/dbsnp/rs2304256"},"validated_against_schema_version":"1.6.2","variant":{"id":"http://identifiers.org/dbsnp/rs2304256","type":"snp single"}}""".stripMargin
    var res = RecordValidator.validate(goodJson)
    assert(res.isValid)

    // Use same record as above but change target id to invalid string
    val badJson =
      """
        |{"access_level":"public","disease":{"id":"http://www.ebi.ac.uk/efo/EFO_0001359"},"evidence":{"gene2variant":{"date_asserted":"2019-09-25T02:26:58+00:00","evidence_codes":["http://purl.obolibrary.org/obo/ECO_0000205","http://identifiers.org/eco/cttv_mapping_pipeline"],"functional_consequence":"http://purl.obolibrary.org/obo/SO_0001583","is_associated":true,"provenance_type":{"database":{"dbxref":{"id":"http://identifiers.org/gwas_catalog","version":"2019-09-25T02:26:58+00:00"},"id":"GWAS Catalog","version":"2019-09-25T02:26:58+00:00"},"expert":{"statement":"Primary submitter of data","status":true}}},"variant2disease":{"confidence_interval":"1.11-1.22","date_asserted":"2019-09-25T02:26:58+00:00","evidence_codes":["http://identifiers.org/eco/GWAS","http://purl.obolibrary.org/obo/ECO_0000205"],"gwas_panel_resolution":2600000,"gwas_sample_size":29835,"is_associated":true,"odds_ratio":"1.16","provenance_type":{"database":{"dbxref":{"id":"http://identifiers.org/gwas_catalog","version":"2019-09-25T02:26:58+00:00"},"id":"GWAS Catalog","version":"2019-09-25T02:26:58+00:00"},"expert":{"statement":"Primary submitter of data","status":true},"literature":{"references":[{"lit_id":"http://europepmc.org/abstract/MED/19966805"}]}},"resource_score":{"exponent":-9,"mantissa":4,"method":{"description":"pvalue for the snp to disease association."},"type":"pvalue","value":4.0E-9},"unique_experiment_reference":"http://europepmc.org/abstract/MED/19966805"}},"literature":{"references":[{"lit_id":"http://europepmc.org/abstract/MED/19966805"}]},"sourceID":"gwas_catalog",
        |"target":{"activity":"http://identifiers.org/cttv.activity/predicted_damaging","id":"THIS_SHOULD_BE_A_URL","target_type":"http://identifiers.org/cttv.target/gene_evidence"},"type":"genetic_association","unique_association_fields":{"disease_id":"http://www.ebi.ac.uk/efo/EFO_0001359","pubmed_refs":"http://europepmc.org/abstract/MED/19966805","pvalue_annotation":"","study_name":"GCST000539","target":"http://identifiers.org/ensembl/ENSG00000105397","variant":"http://identifiers.org/dbsnp/rs2304256"},"validated_against_schema_version":"1.6.2","variant":{"id":"http://identifiers.org/dbsnp/rs2304256","type":"snp single"}}""".stripMargin
    res = RecordValidator.validate(badJson)
    assert(!res.isValid)

    // Custom check
        val json = """{"access_level":"public","disease":{"id":"http://www.ebi.ac.uk/efo/EFO_0004267"},"evidence":{"gene2variant":{"date_asserted":"2019-09-25T02:26:58+00:00","evidence_codes":["http://purl.obolibrary.org/obo/ECO_0000205","http://identifiers.org/eco/cttv_mapping_pipeline"],"functional_consequence":"http://purl.obolibrary.org/obo/SO_0001583","is_associated":true,"provenance_type":{"database":{"dbxref":{"id":"http://identifiers.org/gwas_catalog","version":"2019-09-25T02:26:58+00:00"},"id":"GWAS Catalog","version":"2019-09-25T02:26:58+00:00"},"expert":{"statement":"Primary submitter of data","status":true}}},"variant2disease":{"confidence_interval":"1.11-1.22","date_asserted":"2019-09-25T02:26:58+00:00","evidence_codes":["http://identifiers.org/eco/GWAS","http://purl.obolibrary.org/obo/ECO_0000205"],"gwas_panel_resolution":2600000,"gwas_sample_size":29835,"is_associated":true,"odds_ratio":"1.16","provenance_type":{"database":{"dbxref":{"id":"http://identifiers.org/gwas_catalog","version":"2019-09-25T02:26:58+00:00"},"id":"GWAS Catalog","version":"2019-09-25T02:26:58+00:00"},"expert":{"statement":"Primary submitter of data","status":true},"literature":{"references":[{"lit_id":"http://europepmc.org/abstract/MED/19966805"}]}},"resource_score":{"exponent":-9,"mantissa":4,"method":{"description":"pvalue for the snp to disease association."},"type":"pvalue","value":4.0E-9},"unique_experiment_reference":"http://europepmc.org/abstract/MED/19966805"}},"literature":{"references":[{"lit_id":"http://europepmc.org/abstract/MED/19966805"}]},"sourceID":"BAD_SOURCE_ID","target":{"target_name": "sim005","activity":"http://identifiers.org/cttv.activity/predicted_damaging","id":"http://identifiers.org/ensembl/ENSG999999","target_type":"http://identifiers.org/cttv.target/gene_evidence"},"type":"genetic_association","unique_association_fields":{"disease_id":"http://www.ebi.ac.uk/efo/EFO_0001359","pubmed_refs":"http://europepmc.org/abstract/MED/19966805","pvalue_annotation":"","study_name":"GCST000539","target":"http://identifiers.org/ensembl/ENSG00000105397","variant":"http://identifiers.org/dbsnp/rs2304256"},"validated_against_schema_version":"1.6.2","variant":{"id":"http://identifiers.org/dbsnp/rs2304256","type":"snp single"}}"""
        val r = RecordValidator.validate(json)
        println(r.reason)
        println(r.error)
        assert(r.isValid)
  }
}
