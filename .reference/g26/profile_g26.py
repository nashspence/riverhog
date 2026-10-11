"""Reproduce structural G26 model counters; NOT a full media qualification."""
from __future__ import annotations
import json
import sqlite3
import sys
from factored_index import FactorIndex
from test_factored_index import populated, SOURCE


def run():
    results = []
    for n in (16,32,64,128,256,512):
        with FactorIndex() as db:
            populated(db,n,n)
            counts = db.sizes()
            db.seal(counts,SOURCE)
            result = db.first_support_unguarded([((i,), 's',f'a{i}','recorded-history') for i in reversed(range(n))])
            assert result == {str(i):(0,) for i in range(n)}
            results.append({'profile':'N-members-share-A-assertions','members':n,'assertions':n,
                'logical_pairs_arithmetic_not_materialized':n*n,'physical_rows':sum(counts.values()),
                'tables':counts,'hash_records':db.counts['hash_records'],'hash_bytes':db.counts['hash_bytes'],
                'first_support_nodes':db.counts['first_support_nodes'],'first_support_edges':db.counts['first_support_edges'],
                'returned_members':len(result),'result':'complete-model-output'})
    return {'format':'g26-structural-model-counters/v1','python':sys.version.split()[0],
        'sqlite':sqlite3.sqlite_version,'status':'model-not-production-qualification','profiles':results}


if __name__=='__main__':
    print(json.dumps(run(),indent=2))
