"""Execute reference tests and retain narrowly scoped primitive observations."""
from __future__ import annotations
import argparse
import hashlib
import json
import platform
import sqlite3
import time
import unittest
from pathlib import Path

HERE=Path(__file__).resolve().parent


def main() -> int:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    started=time.perf_counter()
    suite=unittest.defaultTestLoader.discover(str(HERE),pattern='test_*.py')
    result=unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful():return 1
    import test_growth_model as tests
    from growth_model import OperationalModel
    from riverhog_growth_contracts import growth_binding, project_bindings
    projection=project_bindings(tuple(growth_binding(f) for f in (
        OperationalModel.select_due,OperationalModel.select_page,OperationalModel.prune_children)))
    files=['model.py','test_model.py','growth_model.py','test_growth_model.py','run_growth_checks.py',
           'placement-vectors.json']
    payload={
        'format':'reference-tag-subscription-growth-evidence/v1',
        'scope':'external-reference-prototypes-only; not production qualification',
        'audited_main_sha':'e0036f80410a83cc5eaa9d63c17650ffae72e2bd',
        'python':platform.python_version(),'sqlite':sqlite3.sqlite_version,
        'tests_run':result.testsRun,'elapsed_seconds':time.perf_counter()-started,
        'timing_role':'observation-only; no pass/fail timing target',
        'subject_file_sha256':{f:hashlib.sha256((HERE/f).read_bytes()).hexdigest() for f in files},
        'declarations':projection,'actual_primitive_samples':tests.SAMPLES,
        'limits':['No real Riverhog transport, native schedules, payload publication or filesystem ownership tested.',
                  'SQL VM counters are not page-I/O, RSS, end-to-end runtime or whole-operation proof.',
                  'Prototype source is not release-owned code; integration requires owner tests and exact-SHA evidence.'],
    }
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(payload,indent=2,sort_keys=True)+'\n')
    return 0


if __name__=='__main__':raise SystemExit(main())
