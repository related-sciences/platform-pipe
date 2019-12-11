package com.relatedsciences.opentargets.etl.pipeline

import com.relatedsciences.opentargets.etl.Common
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{element_at, split}

object Functions {

  /**
    * Return last component of a URL path */
  def getUrlBasename(col: Column): Column = {
    element_at(split(col, "/"), -1)
  }

  /**
  * Convert disease identifier url to code
    */
  def parseDiseaseIdFromUrl(col: Column): Column = {
    /**
      * This is supposed to be a port of
      * https://github.com/opentargets/data_pipeline/blob/e8372eac48b81a337049dd6b132dd69ff5cc7b64/mrtarget/modules/EFO.py#L21
      * (called from https://github.com/opentargets/data_pipeline/blob/329ff219f9510d137c7609478b05d358c9195579/mrtarget/common/EvidenceString.py#L323)
      * however that domain (identifiers.org) doesn't even occur in the data?!
      *
      * Instead, these URL decompositions were found in raw evidence and this method should proceed based on that:
      * +-------------------+----+-------+
      * |             domain| dir|  count|
      * +-------------------+----+-------+
      * |      www.orpha.net|ORDO| 617976|
      * |      www.ebi.ac.uk| efo|1050929|
      * |purl.obolibrary.org| obo|  61662|
      * +-------------------+----+-------+
      * Example raw disease id: http://www.orpha.net/ORDO/Orphanet_935
      */
    // For now, to maintain with consistency with data_pipeline, even though it appears wrong,
    // don't actually do anything other than get URL basename since the python should be equivalent to that
    getUrlBasename(col)
  }

}
