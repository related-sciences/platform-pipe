package com.relatedsciences.opentargets.etl

import com.relatedsciences.opentargets.etl.pipeline.JsonValidation.{DateValidator, URIFormatValidator}
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
    ).foreach(d =>
      assert(!validator.validate(d).isPresent, s"Date '$d' expected to be valid but was found invalid"))

    // Test invalid dates
    Seq(
      "2019-10-30T",
      "2019-10-30T11",
      "2019-10-30T11:55:4",
      "2019-10-30 11:55:00"  // While common this is not a valid ISO 8601 date (https://en.wikipedia.org/wiki/ISO_8601)
    ).foreach(d =>
      assert(validator.validate(d).isPresent, s"Date '$d' expected to be invalid but was found valid"))
  }

  test("custom uri validator") {
    val validator = new URIFormatValidator()
    // Test valid uris
    Seq(
      "https://somesite.com/file.pdf",
      "/file.pdf",
      "file.pdf"
    ).foreach(d =>
      assert(!validator.validate(d).isPresent, s"URI '$d' expected to be valid but was found invalid"))
    // Test invalid uris
    Seq("").foreach(d =>
      assert(validator.validate(d).isPresent, s"URI '$d' expected to be invalid but was found valid"))
  }
}
