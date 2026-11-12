"""Actual installed native entry counts; maintained audited growth validator."""
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import time

from retained_resource import Resource
from retained_resource.growth import acquire, receive, sync
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import growth_binding,project_bindings,validate_execution_samples


def run():
    start=time.perf_counter()
    operations=(acquire,receive,sync)
    bindings=[growth_binding(op) for op in operations]
    projection=project_bindings(tuple(bindings))
    samples={b.declaration.id:[] for b in bindings}
    widths=set()
    def invoke(op,owner,population,*args,**options):
        before=owner.metrics;op(owner,*args,**options);after=owner.metrics
        identity=growth_binding(op).declaration.id
        samples[identity].append({
            'declaration':identity,
            'declaration_sha256':hashlib.sha256(canonical_json_bytes(projection['declarations'][identity])).hexdigest(),
            'dimensions':{'unrelated_files':population},
            'metrics':{k:after[k]-before[k] for k in ('open_calls','dup_calls','fstat_calls','close_calls','sync_calls')},
            'delegates':{},
        })
    with tempfile.TemporaryDirectory() as td:
        root=Path(td)
        for population in (0,17,257):
            for i in range(population):(root/f'unrelated-{i}').touch(exist_ok=True)
            for extent in (0,17,65537,8*1024*1024):
                path=root/'payload'
                with path.open('wb') as f:f.truncate(extent)
                for kind in (('file','directory') if os.name=='posix' else ('file',)):
                    selected=path if kind=='file' else root
                    encoded=os.fsencode(selected) if os.name=='posix' else str(selected)
                    owner=Resource(kind)
                    invoke(acquire,owner,population,encoded,writable=kind=='file')
                    widths.add(owner.metrics['owner_storage_bytes'])
                    invoke(sync,owner,population)
                    invoke(receive,owner,population)
                    for _ in range(17):invoke(receive,owner,population)
                    assert owner.metrics['close_calls']==1 and owner.received
    for identity,rows in samples.items():validate_execution_samples(projection,identity,rows)
    stream=canonical_json_bytes(samples)
    return {'platform':sys.platform,'elapsed_seconds':time.perf_counter()-start,
            'declarations':projection,'sample_counts':{k:len(v) for k,v in samples.items()},
            'sample_stream_sha256':hashlib.sha256(stream).hexdigest(),
            'native_owner_bytes':sorted(widths),
            'maxima':{k:{m:max(row['metrics'][m] for row in rows) for m in rows[0]['metrics']} for k,rows in samples.items()},
            'scope':'native entry counts, not kernel work, aggregate memory, or complete construction/retirement qualification'}

if __name__=='__main__':print(json.dumps(run(),indent=2))
