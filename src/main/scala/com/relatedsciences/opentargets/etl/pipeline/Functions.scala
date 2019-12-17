package com.relatedsciences.opentargets.etl.pipeline

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{element_at, expr, md5, split, struct, to_json, col}

object Functions {

  /**
    * Return last component of a URL path */
  def getUrlBasename(col: Column): Column = {
    element_at(split(col, "/"), -1)
  }

  /**
    * Convert disease identifier URL to code
    * @param col string field containing URL
    */
  def parseDiseaseIdFromUrl(col: Column): Column = {

    /*
    This is supposed to be a port of
    https://github.com/opentargets/data_pipeline/blob/e8372eac48b81a337049dd6b132dd69ff5cc7b64/mrtarget/modules/EFO.py#L21
    (called from https://github.com/opentargets/data_pipeline/blob/329ff219f9510d137c7609478b05d358c9195579/mrtarget/common/EvidenceString.py#L323)
    however that domain (identifiers.org) doesn't even occur in the data?!

    Instead, these URL decompositions were found in raw evidence and this method should proceed based on that:
    +-------------------+----+-------+
    |             domain| dir|  count|
    +-------------------+----+-------+
    |      www.orpha.net|ORDO| 617976|
    |      www.ebi.ac.uk| efo|1050929|
    |purl.obolibrary.org| obo|  61662|
    +-------------------+----+-------+
    Example raw disease id: http://www.orpha.net/ORDO/Orphanet_935
     */
    // For now, to maintain with consistency with data_pipeline, even though it appears to be out-of-date,
    // don't actually do anything other than get URL basename since the python should be equivalent to that
    getUrlBasename(col)
  }

  /**
    * Extract evidence codes from URLs
    * @param col name of array field containing multiple URLs
    */
  def parseEvidenceCodesFromUrls(col: String): Column = {
    /*
    This is supposed to be a port of
    https://github.com/opentargets/data_pipeline/blob/329ff219f9510d137c7609478b05d358c9195579/mrtarget/common/EvidenceString.py#L256
    however many of those patterns do not occur.

    These URL decompositions, as distinct counts, were found in raw evidence records
    (at TOW, only 19 different codes are used):
    +------------------------+--------+---------------------+-----+
    |domain                  |dir     |name                 |count|
    +------------------------+--------+---------------------+-----+
    |identifiers.org         |eco     |ECO:0000179          |1    |
    |purl.obolibrary.org     |obo     |ECO_0000205          |3    |
    |identifiers.org         |eco     |cttv_mapping_pipeline|1    |
    |www.targetvalidation.org|evidence|literature_mining    |1    |
    |purl.obolibrary.org     |obo     |ECO_0000213          |1    |
    |identifiers.org         |eco     |target_drug          |1    |
    |identifiers.org         |eco     |drug_disease         |1    |
    |identifiers.org         |eco     |GWAS                 |1    |
    |purl.obolibrary.org     |obo     |ECO_0000058          |1    |
    |purl.obolibrary.org     |obo     |ECO_0000295          |1    |
    |purl.obolibrary.org     |obo     |ECO_0000053          |1    |
    |identifiers.org         |eco     |PheWAS               |1    |
    |purl.obolibrary.org     |obo     |ECO_0000305          |2    |
    |purl.obolibrary.org     |obo     |ECO_0000303          |1    |
    |purl.obolibrary.org     |obo     |ECO_0000250          |1    |
    |purl.obolibrary.org     |obo     |ECO_0000204          |1    |
    +------------------------+--------+---------------------+-----+
     */
    // For some reason, the "transform" function isn't exposed from the Scala API in Spark 2.4.4
    // so the only way to apply a function to array elements without a UDF seems to be using the SQL expr API
    expr(
      """
        |transform(%s, v ->
        | case when v like "%%/identifiers.org/eco/%%"
        |   then regexp_replace(element_at(split(v, "/"), -1), "ECO:", "ECO_")
        | else element_at(split(v, "/"), -1)
        | end)
        |""".stripMargin.format(col)
    )
  }

  /**
  * Add record id as MD5 of json representation of unique association fields
    *
    * Note: A side effect of this function includes redefining the unique_association_fields column
    * to precisely represent what was used to construct the id field.
    * @param name name of resulting id column (default "id")
    * @param df evidence dataset with at least `unique_association_fields` and `sourceID` columns
    */
  def addEvidenceRecordId(name: String = "id")(df: DataFrame): DataFrame = {
    // See: https://github.com/opentargets/data_pipeline/blob/329ff219f9510d137c7609478b05d358c9195579/mrtarget/modules/Evidences.py#L190
    val fields = df
      .select("unique_association_fields.*").columns.toSeq
      .filter(_ != "datasource") :+ "sourceID"
    val cols = fields.sortWith(_ < _).map(c =>
      if (c != "sourceID") col("unique_association_fields." + c).as(c)
      else col("sourceID").as("datasource")
    )
    df
      // Re-define UAF with data source (and ensure sorting of struct fields)
      .withColumn("unique_association_fields", struct(cols:_*))
      // Generate id as md5 of key-sorted json
      .withColumn(name, md5(to_json(col("unique_association_fields"))))
  }
}
