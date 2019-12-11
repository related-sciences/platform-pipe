## Input Files for Pipeline Tests

### Raw Evidence

The data in evidence_raw.json contains an extract with real data as well as some simulated data to force corner cases (simulated-records.json).

The real data extracted was done by notebooks/testing/evidence-test-data-extractor.ipynb.

The simulated examples have the following changes:
line 1 - target.id is changed to http://identifiers.org/ensembl/ENSG999999 which will clear schema checks but not match
line 2 - disease.id changed to http://www.ebi.ac.uk/efo/BAD_EFO_ID (which will also pass schema validation and not match)