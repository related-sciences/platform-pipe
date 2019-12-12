## Input Files for Pipeline Tests

### Raw Evidence

The data in evidence_raw.json contains an extract with real data as well as some simulated data to force corner cases (simulated-records.json).

The real data extracted was done by notebooks/testing/evidence-test-data-extractor.ipynb.

The simulated examples have the following changes:
line 1 - target.id is changed to http://identifiers.org/ensembl/ENSG999999 which will clear schema checks but not match
line 2 - disease.id changed to http://www.ebi.ac.uk/efo/BAD_EFO_ID (which will also pass schema validation and not match)
line 3 - source changed to expression_atlas and gene id changed to ENSG00000240253 as this will have a biotype to be excluded
line 4 - source changed to expression_atlas and gene id changed to PCSK9 (ENSG00000169174) so it will not have a biotype exclusion