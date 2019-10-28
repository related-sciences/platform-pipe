package com.relatedsciences.opentargets.pipeline.schema

object DataSource extends Enumeration {
  val europepmc, phewas_catalog, twentythreeandme, uniprot_somatic, progeny, sysbio, gene2phenotype,
      crispr, intogen, uniprot_literature, eva_somatic, reactome, genomics_england, uniprot,
      cancer_gene_census, slapenrich, eva, gwas_catalog, expression_atlas, chembl, phenodigm = Value
}
