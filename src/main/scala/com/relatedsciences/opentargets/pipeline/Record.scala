package com.relatedsciences.opentargets.pipeline

import com.relatedsciences.opentargets.pipeline.schema.Fields.{FieldName, FieldPath}
import org.apache.spark.sql.Row

class Record(val id: String, val typeId: String, val sourceId: String, val row: Row) {
  def get[T](field: FieldName.Value): Option[T] = {
    val f = FieldName.flatName(field)
    if (row.isNullAt(row.fieldIndex(f))) None else Some(row.getAs[T](f))
  }
  def exists(field: FieldPath.Value): Boolean = {
    row.getAs(FieldPath.flatName(field))
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
