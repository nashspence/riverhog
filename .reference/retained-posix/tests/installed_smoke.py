"""Run with -I outside the checkout in a normal (non-fault) installed wheel."""
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile

import retained_posix
import retained_posix._native as native
from retained_posix import Borrow,Group,Resource
from retained_posix.extent import ExtentTurn

assert not hasattr(Resource('file'),'_test_fault')
assert '.reference' not in retained_posix.__file__
assert not any('riverhog987/clone' in p for p in sys.path)
with tempfile.TemporaryDirectory() as d:
    root=Path(d); data=root/'data'; data.write_bytes(b'abcdefghijk'); data.chmod(0o600)
    g=Group(4); parent=g.resource('directory'); parent.acquire(os.fsencode(root))
    child=g.resource('file'); child.acquire(b'data',parent=parent)
    b=g.borrow(child); b.acquire(); old=b.fileno(); b.close()
    try:
        child.close()
        raise RuntimeError('lost caller reply after actual native close')
    except RuntimeError: pass
    other=os.open(data,os.O_RDONLY)
    if other!=old: os.dup2(other,old,inheritable=False); os.close(other)
    try:
        assert g.close_step(4); os.fstat(old)
    finally: os.close(old)
    t=ExtentTurn(root,names=('data',),octet_budget=3)
    result=t.advance(); assert t.received and result.removed_octets==3
    assert data.read_bytes()==b'abcdefgh'
print(json.dumps({'module':retained_posix.__file__,'native':native.__file__,
                  'native_sha256':hashlib.sha256(Path(native.__file__).read_bytes()).hexdigest(),
                  'fault_hooks':False,'isolated':bool(sys.flags.isolated),
                  'success':True,'platform':sys.platform,'python':sys.version},indent=2))
