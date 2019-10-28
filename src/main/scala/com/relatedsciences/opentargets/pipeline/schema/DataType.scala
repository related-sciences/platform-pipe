package com.relatedsciences.opentargets.pipeline.schema

object DataType extends Enumeration {
  val known_drug, rna_expression, animal_model, somatic_mutation, literature, genetic_association,
      affected_pathway = Value
}
