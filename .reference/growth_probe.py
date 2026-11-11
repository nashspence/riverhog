"""Execute native counters and validate the source-owned maintained declarations."""
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time

from retained_posix import Resource
from retained_posix.growth import acquire,receive
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import growth_binding,project_bindings,validate_execution_samples


def run():
    start=time.perf_counter()
    bindings=[growth_binding(acquire),growth_binding(receive)]
    projection=project_bindings(tuple(bindings))
    samples={b.declaration.id:[] for b in bindings}
    widths=set()
    def invoke(operation,owner,backlog,*args,**kwargs):
        before=owner.metrics
        operation(owner,*args,**kwargs)
        after=owner.metrics
        b=growth_binding(operation); identity=b.declaration.id
        row={"declaration":identity,
             "declaration_sha256":hashlib.sha256(canonical_json_bytes(projection['declarations'][identity])).hexdigest(),
             "dimensions":{"unrelated_files":backlog},
             "metrics":{k:after[k]-before[k] for k in ('open_calls','dup_calls','fstat_calls','close_calls')},
             "delegates":{}}
        samples[identity].append(row)
    with tempfile.TemporaryDirectory() as d:
        root=Path(d)
        for backlog in (0,17,257):
            for i in range(backlog):(root/f'unrelated-{i}').touch(exist_ok=True)
            for extent in (0,17,65537,8*1024*1024):
                p=root/'data'
                with p.open('wb') as f:f.truncate(extent)
                for kind in ('file','directory'):
                    r=Resource(kind)
                    invoke(acquire,r,backlog,os.fsencode(p if kind=='file' else root))
                    widths.add(r.metrics['owner_storage_bytes'])
                    invoke(receive,r,backlog)
                    for _ in range(17):invoke(receive,r,backlog)
                    assert r.metrics['close_calls']==1 and r.received
    for identity,rows in samples.items():validate_execution_samples(projection,identity,rows)
    return {"elapsed_seconds":time.perf_counter()-start,"declarations":projection,
            "samples":samples,"native_owner_storage_bytes":sorted(widths),
            "scope":"actual reference native counters; maintained growth validator at audited source; not product closure/audit qualification"}

if __name__=='__main__':print(json.dumps(run(),indent=2))
