# Data Record EDA

This contains helpful query results for understanding OT data.  All of these are in regards to either the 19.09 or 19.11 releases.

## Gene Data

### UniProt

Uniprot ids are never null, though they are often blank strings
```
df.filter($"uniprot_id".isNull).count()
> 0

df.withColumn("isBlank", $"uniprot_id" === "").groupBy("isBlank").count().show()
+-------+-----+
|isBlank|count|
+-------+-----+
|   true|40475|
|  false|20089|
+-------+-----+
```

Accessions include the primary id, or are empty if primary blank:
```
df.select("uniprot_id", "uniprot_accessions").show(5, 100)
+----------+----------------------------------------+
|uniprot_id|                      uniprot_accessions|
+----------+----------------------------------------+
|          |                                      []|
|    Q8N158|                        [Q8N158, A4D2A7]|
|    Q96RY5|[Q9P2C1, A8MZL1, Q96RY5, B1AJY1, Q8NDN1]|
|    B1AL46|                        [A6NHL0, B1AL46]|
|    P49116|        [B6ZGT8, A8K3H5, P49116, P55092]|
+----------+----------------------------------------+
```

## Non Reference Genes

This shows how often gene ids in raw evidence data match to the non reference gene mapping (i.e. genes_with_non_reference_ensembl_ids.tsv):

```
val df = ss.read.json("evidence-files")
val dfa = ss.read.csv("genes_with_non_reference_ensembl_ids.tsv")

// Matches of unique Ensembl ids in evidence to unique Ensembl ids in non ref mappping:
df.select(element_at(split($"target.id", "/"), -1).as("eid")).dropDuplicates()
    .join(dfa.select($"ensembl_gene_id".as("eid"), $"gene_symbol").dropDuplicates(), Seq("eid"), "left")
    .withColumn("na", $"gene_symbol".isNull).groupBy("m").count().show
+-----+-----+
|   na|count|
+-----+-----+
| true|36745|
|false|  225|
+-----+-----+
==> .6% match rate

// Matches of ALL Ensembl ids in evidence to unique Ensembl ids in non ref mappping:
df.select(element_at(split($"target.id", "/"), -1).as("eid"))
    .join(dfa.select($"ensembl_gene_id".as("eid"), $"gene_symbol").dropDuplicates(), Seq("eid"), "left")
    .withColumn("na", $"gene_symbol".isNull).groupBy("m").count().show
+-----+-------+
|   na|  count|
+-----+-------+
| true|1701376|
|false|  29192|
+-----+-------+
==> 1.6% match rate
```

## Raw Evidence

Queries on raw evidence string files in GS (based on ALL evidence strings and a EPMC sample (1k))

### UniProt IDs

data_pipeline refers to splitting uniprot ids on hyphen, yet this doesn't seem to occur in the data:

see: https://github.com/opentargets/data_pipeline/blob/329ff219f9510d137c7609478b05d358c9195579/mrtarget/common/EvidenceString.py#L276 
```
df.filter($"target.id".contains("uniprot") && $"target.id".contains("-")).select("target.id").show(10, 100)
+---+
| id|
+---+
+---+
```

They match this format instead:
```
df.filter($"target.id".contains("uniprot") && $"target.id".contains("/")).select("target.id").show(10, 100)
+-------------------------------------+
|                                   id|
+-------------------------------------+
|http://identifiers.org/uniprot/P23219|
|http://identifiers.org/uniprot/P35354|
|http://identifiers.org/uniprot/P23219|
|http://identifiers.org/uniprot/P35354|
|http://identifiers.org/uniprot/P10275|
+-------------------------------------+
```

### Disease IDs

To better determine how different disease ids are sourced, this shows how the raw vaules are distributed:

```$xslt
val df = ss.read.json(Paths.get(EXTRACT_DIR.resolve("evidence_raw.json").toString))
// Ids are urls like: http://www.orpha.net/ORDO/Orphanet_1572 
df.withColumn("m", split($"disease.id", "/")).select(
    $"m".getItem(2).as("domain"),
    $"m".getItem(3).as("dir"),
    $"m".getItem(4).as("id")
).groupBy("domain", "dir").count().show

+-------------------+----+-------+
|             domain| dir|  count|
+-------------------+----+-------+
|      www.orpha.net|ORDO| 617976|
|      www.ebi.ac.uk| efo|1050929|
|purl.obolibrary.org| obo|  61662|
|               null|null|      1|
+-------------------+----+-------+
```

## Prepared Evidence

These query results contain useful high level information about all evidence records (after importation) and their respective sources.

All queries are across all evidence strings (except EPMC @ TOW).

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