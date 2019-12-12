package com.relatedsciences.opentargets.etl.schema

object DataType extends Enumeration {
  val known_drug, rna_expression, animal_model, somatic_mutation, literature, genetic_association,
      genetic_literature, affected_pathway = Value
}
