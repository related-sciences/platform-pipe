

object FieldName extends Enumeration {
  val evidence$drug2clinic$resource_score$value,
  evidence$target2drug$resource_score$value, evidence$log2_fold_change$value,
  evidence$log2_fold_change$percentile_rank, evidence$resource_score$value =
    Value

  def pathName(value: Value): String =
    value.toString.replace("$", ".")

  def flatName(value: Value): String =
    value.toString.replace("$", "_")
}

val evidence_paths = FieldName.values.map(FieldName.pathName)
val evidence_alias = FieldName.values.map(FieldName.flatName)
val evidence_cols = (evidence_paths, evidence_alias).zipped.toList
  .sorted.map((t) => t._1 + ":" + t._2)
println(evidence_paths)
println(evidence_alias)
println(evidence_cols(0))