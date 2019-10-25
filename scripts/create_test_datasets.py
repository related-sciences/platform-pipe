import collections


def get_record_iterator():
    keys = [
        'somatic_mutation',
        'animal_model',
        'rna_expression',
        'known_drug',
        'log2_fold_change',
        'drug2clinic',
        'target2drug',
        'gene2variant'
    ]
    with open('/home/eczech/data/ot/extract/evidence.json', 'r') as fd:
        for line in fd:
            for k in keys:
                if k in line:
                    yield k, line
                    break


def get_record_sample(n):
    iterator = get_record_iterator()
    records = {}
    for key, line in iterator:
        if key not in records:
            records[key] = collections.deque(maxlen=n)
        records[key].append(line)
    return records


def write_records(records):
    print("Writing records with counts: {}".format({k: len(v) for k, v in records.items()}))
    with open('/home/eczech/repos/ot-scoring/src/test/resources/pipeline_test/input/evidence.json', 'w') as fd:
        for k, records in records.items():
            for r in records:
                fd.write(r)


if __name__ == '__main__':
    write_records(get_record_sample(10))
