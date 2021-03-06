#!/usr/bin/env python3
# Extract Elasticsearch indexes loaded by data_pipeline into separate files
#
# Usage (in ot-import container):
# cd ~/repos/platform-pipe/scripts
# ./data_pipeline_extract.py --index=master_evidence-data --output-dir=~/data/ot/extract --output-filename=evidence.json
# ./data_pipeline_extract.py --index=master_gene-data --output-dir=~/data/ot/extract --output-filename=gene.json
# ./data_pipeline_extract.py --index=master_association-data --output-dir=~/data/ot/extract --output-filename=association.json
# ./data_pipeline_extract.py --index=master_efo-data --output-dir=~/data/ot/extract --output-filename=efo.json --id-field-name=id
# ./data_pipeline_extract.py --index=master_eco-data --output-dir=~/data/ot/extract --output-filename=eco.json
from mrtarget.common.connection import new_es_client
from elasticsearch import helpers
from pathlib import Path
import more_itertools
import tqdm
import argparse
import json
import os
import logging

es = new_es_client('http://elasticsearch:9200')


def get_record_iterator(index, id_field, batch_size=10000):
    # Setup scanner for entire index
    query = {"query": {"match_all": {}}}
    res = helpers.scan(es, query, index=index, size=batch_size, scroll='1h')
    for batch in more_itertools.chunked(tqdm.tqdm(res), batch_size):
        for r in batch:
            rec = r['_source']
            if id_field:
                rec[id_field] = r['_id']
            yield rec

def export(index, out_file, id_field):
    logging.info(f'Beginning export to {out_file}')
    iterator = get_record_iterator(index, id_field)

    # Delete output file if it exists
    if out_file.exists():
        out_file.unlink()
    if not out_file.parent.exists():
        out_file.parent.mkdir(parents=True)

    # Write objects as json
    with out_file.open('w+') as fd:
        for r in iterator:
            fd.write(json.dumps(r) + os.linesep)
    logging.info(f'Export to {out_file} complete')


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--index', help='ES index name')
    parser.add_argument('--output-dir', default='~/data/ot/extract', help='Output directory for file')
    parser.add_argument('--output-filename', default='', help='Name of output file (default is {index}.jsonl)')
    parser.add_argument('--id-field-name', default='', help='Name of field to assign ES _id field to (default is to exclude it)')
    args = parser.parse_args()

    index = args.index
    out_dir = Path(args.output_dir).expanduser()
    out_file = args.output_filename if args.output_filename else index + '.jsonl'
    out_file = out_dir / out_file
    id_field = args.id_field_name

    export(index, out_file, id_field)


if __name__ == '__main__':
    logging.basicConfig(level='INFO')
    run()
