package com.relatedsciences.opentargets.etl

import com.relatedsciences.opentargets.etl.schema.Fields.FieldName
import org.apache.spark.sql.Row

/**
  * Model object for evidence data
  *
  * @param id identifier for evidence string
  * @param typeId type associated with data (e.g. rna_expression, somatic_mutation, genetic_association)
  * @param sourceId source of data for the given type (e.g. gwas_catalog, twentythreeandme, sysbio)
  * @param row evidence data
  */
class Record(val id: String, val typeId: String, val sourceId: String, val row: Row) {
  def get[T](field: FieldName.Val): Option[T] = {
    val f = FieldName.flatName(field)
    if (row.isNullAt(row.fieldIndex(f))) None else Some(row.getAs[T](f))
  }
  def exists(field: FieldName.Val): Boolean = {
    require(field.isPath)
    row.getAs(FieldName.flatName(field))
  }
  override def toString: String = {
    "{id: %s, typeId: %s, sourceId: %s, data: %s}".format(
      this.id,
      this.typeId,
      this.sourceId,
      this.row.getValuesMap(this.row.schema.fieldNames)
    )
  }
}
