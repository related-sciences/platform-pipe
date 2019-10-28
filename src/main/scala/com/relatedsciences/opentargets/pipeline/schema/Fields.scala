package com.relatedsciences.opentargets.pipeline.schema

object Fields {

  /**
   * Static field enum representing flat paths to values within evidence strings
   *
   * Note: These fields are used in the extraction pipeline to form a union of columns
   * to extract from raw evidence and as such, any field necessary for a score
   * calculation must be registered here.
   */
  object FieldName extends Enumeration {
    val evidence$drug2clinic$resource_score$value,
    evidence$target2drug$resource_score$value, evidence$log2_fold_change$value,
    evidence$log2_fold_change$percentile_rank, evidence$resource_score$value,
    evidence$resource_score$type, unique_association_fields$cases,
    evidence$variant2disease$resource_score$type,
    evidence$gene2variant$resource_score$value,
    evidence$variant2disease$gwas_sample_size,
    evidence$disease_model_association$resource_score$value,
    evidence$known_mutations, evidence$variant2disease$resource_score$value =
      Value

    def pathName(value: Value): String =
      value.toString.replace("$", ".")

    def flatName(value: Value): String =
      value.toString.replace("$", "_")
  }

  /**
   * Static field enum representing flat paths for which existence of information at that path must be inferred
   *
   * Note: These fields are used in the extraction pipeline to form a union of columns
   * to extract from raw evidence and as such, any field necessary for a score
   * calculation must be registered here.
   */
  object FieldPath extends Enumeration {
    val evidence$gene2variant, evidence$known_mutations = Value

    def pathName(value: Value): String =
      value.toString.replace("$", ".")

    def flatName(value: Value): String =
      value.toString.replace("$", "_") + "_exists"
  }

}
