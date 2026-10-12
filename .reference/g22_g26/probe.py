"""Run bounded-work component counters; no product qualification or timing target."""
from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
from pathlib import Path

from model import DagWork, FactWork, FRAGMENT
from sha256_state import Sha256


def run():
    observations=[]
    with tempfile.TemporaryDirectory() as directory:
        root=Path(directory)
        for size in (128,512,2048):
            graph=root/f'g{size}'
            g=DagWork(graph)
            for start in range(0,size,31):
                g.add([(0,i+1) for i in range(start,min(size,start+31))],31)
            g.seal();ticks=maximum=0
            while g.status()[0] not in ('done','cyclic'):
                g=DagWork(graph) # actual reopen across every quantum
                maximum=max(maximum,g.step(17));ticks+=1
            status=g.status()
            fpath=root/f'f{size}';f=FactWork(fpath,size)
            for start in range(0,size,31):
                f.stage([(i,0,f'fact-{i}'.encode(),len(f'fact-{i}')) for i in range(start,min(size,start+31))],
                        row_budget=31,byte_budget=31*FRAGMENT)
            f.seal();fticks=fmaximum=0
            while f.status()[0]!='done':
                f=FactWork(fpath);fmaximum=max(fmaximum,f.step(17));fticks+=1
            observations.append({'n':size,'dag_vertices':status[1],'dag_edge_visits':status[3],
                'dag_quanta':ticks,'dag_max_transitions_per_quantum':maximum,
                'merkle_leaves':f.status()[2],'merkle_quanta':fticks,
                'merkle_max_transitions_per_quantum':fmaximum,'model_root':f.root().hex()})
    h=Sha256();data=b'x'*16387;maxstate=0
    for start in range(0,len(data),257):
        h.update(data[start:start+257]);maxstate=max(maxstate,len(h.export()))
        h=Sha256.restore(h.export())
    assert h.digest()==hashlib.sha256(data).digest()
    return {'status':'diagnostic-only','sqlite':sqlite3.sqlite_version,
            'diagnostic_fragment_bytes':FRAGMENT,'observations':observations,
            'sha256':{'bytes':len(data),'max_private_state_bytes':maxstate,
                      'digest':h.digest().hex(),'native_hashlib_equal':True},
            'limits':'Synthetic integer DAG and fact bytes. No native semantic validator, production source adapter, PostgreSQL, pool, fsync power-loss or runtime target qualification.'}


if __name__=='__main__':
    print(json.dumps(run(),indent=2,sort_keys=True))
