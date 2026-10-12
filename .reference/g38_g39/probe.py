"""Reproduce source/query diagnostics and model work counts, not qualification."""
import argparse
import hashlib
import json
from pathlib import Path
import tempfile
from reference import Store, Grant
from source_probe import native_delivery, native_unresolved_taint


def run(repo):
    result={'format':'g38-g39-diagnostic-counts/v1','native_delivery':[], 'native_taint':[], 'model':[]}
    for n in (128,512,2048):
        result['native_taint'].append(native_unresolved_taint(repo,n))
        for kind in ('whole-scope','independent-subjects'):
            result['native_delivery'].append(native_delivery(repo,n,kind))
        with tempfile.TemporaryDirectory() as root:
            store=Store(Path(root)/'work.db');store.begin_work('j')
            for i in range(n):
                s=f'{i:064x}';store.asked_row('j',s,s);store.answer_row('j',s,s,'complete')
            for i in range(n):store.relation('j',i,None,None,False)
            store.seal_stage('j',independently_validated=True)
            calls=0;visits=0;max_work=0
            while True:
                count=store.check_step('j',1,17)
                calls+=1;visits+=count;max_work=max(max_work,count)
                if not count:break
            store.publish('j',1,current_authority=True)
            with store.db() as db:
                source_rows=db.execute('SELECT count(*) FROM source_relation').fetchone()[0]
                taint=db.execute('SELECT taint FROM work').fetchone()[0]
                eager=db.execute("SELECT count(*) FROM answer WHERE status='unsupported'").fetchone()[0]
            result['model'].append({'subjects':n,'relation_records':source_rows,
                'whole_scope_taint_markers':taint,'eager_per_subject_taint_updates':eager,
                'coverage_subjects_visited':visits,'coverage_step_calls':calls,
                'max_subjects_per_coverage_step':max_work,
                'status_bytes':len(store.status('j')),'open_connections_after_step':store.connections})
    result['limits']=['model rows are bounded toy identities, not native facts',
                      'SQL machine/PG/workload/time targets not established',
                      'source typed deserialization and physical seal are instrumented boundaries']
    return result

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--repo',type=Path,required=True)
    args=parser.parse_args();print(json.dumps(run(args.repo),indent=2,sort_keys=True))
