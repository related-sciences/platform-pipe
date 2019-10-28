## Evidence String EDA

These query results contain useful high level information about all evidence strings and their respective sources.

All queries are across all evidence strings.

### Sources and Types

```
> df.groupBy("source_id").count().orderBy("count").show()
+------------------+------+
|         source_id| count|
+------------------+------+
|   uniprot_somatic|   284|
|           progeny|   308|
|            sysbio|   408|
|    gene2phenotype|  1586|
|            crispr|  1659|
|           intogen|  2375|
|uniprot_literature|  4553|
|       eva_somatic|  7585|
|          reactome| 10159|
|  genomics_england| 10528|
|           uniprot| 30480|
|    phewas_catalog| 55987|
|cancer_gene_census| 60310|
|        slapenrich| 74570|
|               eva| 96734|
|      gwas_catalog|180984|
|  expression_atlas|204229|
|            chembl|383122|
|         phenodigm|500683|
+------------------+------+
```

```
> df.groupBy("type").count().orderBy("count").show()
+-------------------+------+
|               type| count|
+-------------------+------+
|   somatic_mutation| 70554|
|   affected_pathway| 87104|
|     rna_expression|204229|
|genetic_association|380852|
|         known_drug|383122|
|       animal_model|500683|
+-------------------+------+
```

```
> df.groupBy("source_id", "type_id").count().orderBy("count").show()
+------------------+-------------------+------+
|         source_id|            type_id| count|
+------------------+-------------------+------+
|   uniprot_somatic|   somatic_mutation|   284|
|           progeny|   affected_pathway|   308|
|            sysbio|   affected_pathway|   408|
|    gene2phenotype|genetic_association|  1586|
|            crispr|   affected_pathway|  1659|
|           intogen|   somatic_mutation|  2375|
|uniprot_literature|genetic_association|  4553|
|       eva_somatic|   somatic_mutation|  7585|
|          reactome|   affected_pathway| 10159|
|  genomics_england|genetic_association| 10528|
|           uniprot|genetic_association| 30480|
|    phewas_catalog|genetic_association| 55987|
|cancer_gene_census|   somatic_mutation| 60310|
|        slapenrich|   affected_pathway| 74570|
|               eva|genetic_association| 96734|
|      gwas_catalog|genetic_association|180984|
|  expression_atlas|     rna_expression|204229|
|            chembl|         known_drug|383122|
|         phenodigm|       animal_model|500683|
+------------------+-------------------+------+
```