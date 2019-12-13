## Input Files for Pipeline Tests

### Raw Evidence

The data in evidence_raw.json contains an extract with real data as well as some simulated data to force corner cases (simulated-records.json).

The real data extracted was done by notebooks/testing/evidence-test-data-extractor.ipynb.

The simulated examples have the following changes:
- line 1 - Target validation - sim001-badtargetid
    - target.id is changed to http://identifiers.org/ensembl/ENSG999999 which will clear schema checks but not match
- line 2 - Disease validation - sim002-badefoid
    disease.id changed to http://www.ebi.ac.uk/efo/BAD_EFO_ID (which will also pass schema validation and not match)
- line 3 - Biotype filtering - sim003-excludedbiotype
    - source changed to expression_atlas and gene id changed to ENSG00000240253 as this will have a biotype to be excluded
- line 4 - Biotype filtering - sim004-includedbiotype
    - source changed to expression_atlas and gene id changed to PCSK9 (ENSG00000169174) so it will NOT 
    have a biotype exclusion
- line 5 - Source validation - sim005-badsource
    - clone of line 1 with source changed to "bad_source_id" (must be lower case to pass schema validation)
- line 6 - ECO score enforcement - sim006-ecoscore-rsdiff
    - valid genetic_association record with gene2variant.resource_score added having type "probability" and 
    value .0001 not matching that for the value associated with the gene2variant.functional_consequence 
    ECO code (SO_0001060 -> .5) in static value mapping (see https://storage.googleapis.com/open-targets-data-releases/19.11/input/annotation-files/eco_scores-2019-10-31.tsv)
- line 7 - ECO score enforcement - sim007-ecoscore-rsnull
    - same as 6 with gene2variant.resource_score removed 
- line 8 - ECO score enforcement - sim008-ecoscore-rsequal
    - Same as 6 with gene2variant.resource_score set to equal value, i.e. .5   
- line 9 - ECO score enforcement - sim009-ecoscore-badecoid
    - Same as 6 with value changed to .3 and ECO code at gene2variant.functional_consequence changed 
    to SO_0000000 to test that unmapped ECO codes result in no override  
  

In each of the above, a field is added at target.target_name (as it is not constrained by the json schema)