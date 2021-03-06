set -e
URLS="https://storage.googleapis.com/open-targets-data-releases/19.11/input/evidence-files/uniprot-2019-11-21.json.gz
https://storage.googleapis.com/open-targets-data-releases/19.11/input/evidence-files/slapenrich-2018-11-29.json.gz
https://storage.googleapis.com/open-targets-data-releases/19.11/input/evidence-files/crispr-2019-08-21.json.gz
https://storage.googleapis.com/open-targets-data-releases/19.11/input/evidence-files/gwas-2019-10-28.json.gz
https://storage.googleapis.com/open-targets-data-releases/19.11/input/evidence-files/eva-2019-10-17.json.gz
https://storage.googleapis.com/open-targets-data-releases/19.11/input/evidence-files/cosmic-2019-11-13.json.gz
https://storage.googleapis.com/open-targets-data-releases/19.11/input/evidence-files/sysbio-2019-01-31.json.gz
https://storage.googleapis.com/open-targets-data-releases/19.11/input/evidence-files/progeny-2018-07-23.json.gz
https://storage.googleapis.com/open-targets-data-releases/19.11/input/evidence-files/phenodigm-2019-10-30.json.gz
https://storage.googleapis.com/open-targets-data-releases/19.11/input/evidence-files/intogen-2019-08-16.json.gz
https://storage.googleapis.com/open-targets-data-releases/19.11/input/evidence-files/genomics_england-2018-10-02.json.gz
https://storage.googleapis.com/open-targets-data-releases/19.11/input/evidence-files/reactome-2019-10-28.json.gz
https://storage.googleapis.com/open-targets-data-releases/19.11/input/evidence-files/chembl-2019-08-16.json.gz
https://storage.googleapis.com/open-targets-data-releases/19.11/input/evidence-files/gene2phenotype-2019-08-19.json.gz
https://storage.googleapis.com/open-targets-data-releases/19.11/input/evidence-files/atlas-2019-10-31.json.gz
https://storage.googleapis.com/open-targets-data-releases/19.11/input/evidence-files/phewas_catalog-2018-11-28.json.gz
https://storage.googleapis.com/open-targets-data-releases/19.11/input/evidence-files/epmc-2019-10-28.json.gz"

LIMIT=""
DEST="/data/disk1/ot/dev/extract/evidence_raw.json"
mkdir -p $DEST
for url in $URLS; do
  filename=$(basename -- "$url")
  echo "Downloading data from $url to $DEST/$filename"
  if [ -z "$LIMIT" ]; then
    cmd="curl -s -o $DEST/$filename $url"
  else
    cmd="curl -s $url | gzip -dc | head -n $LIMIT | gzip -c > $DEST/$filename"
  fi
  # This is optional and can be omitted to save space, but it will substantially improve
  # pipeline performance if the raw evidence files are splittable
  gzip -d $DEST/$filename
done;