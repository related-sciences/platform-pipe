package com.relatedsciences.opentargets.pipeline.schema
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object Fields {

  /**
    * Static field enum representing flat paths to values within nested evidence objects
    *
    * Note: These fields are used in the extraction pipeline to form a union of columns
    * to extract from raw evidence and as such, any field necessary for a score
    * calculation must be registered here.
    */
  object FieldName extends Enumeration {
    case class Val(isPath: Boolean) extends super.Val
    val evidence$drug2clinic$resource_score$value, evidence$target2drug$resource_score$value,
        evidence$log2_fold_change$value, evidence$log2_fold_change$percentile_rank,
        evidence$resource_score$value, evidence$resource_score$type,
        unique_association_fields$cases, evidence$variant2disease$resource_score$type,
        evidence$gene2variant$resource_score$value, evidence$variant2disease$gwas_sample_size,
        evidence$disease_model_association$resource_score$value, evidence$known_mutations,
        evidence$variant2disease$resource_score$value = Val(false)
    val evidence$gene2variant$exists                  = Val(true)

    def pathName(value: Val): String = {
      var name = value.toString.replace("$", ".")
      if (value.isPath) {
        name = name.replaceAll("\\.exists$", "")
      }
      name
    }

    def flatName(value: Val): String =
      value.toString.replace("$", "_")
  }

  def toColumns(fields: Iterable[FieldName.Val]): Iterable[Column] = {
    fields
      .map(f => (FieldName.pathName(f), FieldName.flatName(f), f.isPath))
      .map({
        case (pName, fName, isPath) =>
          if (!isPath) col(pName).as(fName)
          else col(pName).isNotNull.as(fName)
      })
  }

  def allColumns: Iterable[Column] = {
    toColumns(FieldName.values.toList.map(_.asInstanceOf[FieldName.Val]))
  }
}
