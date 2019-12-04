#!/usr/bin/env python3
# Extract Elasticsearch indexes loaded by data_pipeline into separate files
#
# Usage (in ot-import container):
# cd ~/repos/ot-scoring/scripts
# ./data_pipeline_extract.py --index=master_evidence-data --output-dir=~/data/ot/extract --output-filename=evidence.json
# ./data_pipeline_extract.py --index=master_gene-data --output-dir=~/data/ot/extract --output-filename=gene.json
# ./data_pipeline_extract.py --index=master_association-data --output-dir=~/data/ot/extract --output-filename=association.json
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


def get_record_iterator(index, batch_size=10000):
    # Setup scanner for entire index
    query = {"query": {"match_all": {}}}
    res = helpers.scan(es, query, index=index, size=batch_size, scroll='1h')
    for batch in more_itertools.chunked(tqdm.tqdm(res), batch_size):
        for r in batch:
            yield r['_source']


def export(index, out_file):
    logging.info(f'Beginning export to {out_file}')
    iterator = get_record_iterator(index)

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
    args = parser.parse_args()

    index = args.index
    out_dir = Path(args.output_dir).expanduser()
    out_file = args.output_filename if args.output_filename else index + '.jsonl'
    out_file = out_dir / out_file

    export(index, out_file)


if __name__ == '__main__':
    logging.basicConfig(level='INFO')
    run()
