"""Full-checkout bookkeeping and contract-independence regressions.

These tests need the real repository and its locked dependency environment.
They are not part of the standalone bundle validation claim.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from scripts import performance_objectives as performance

ROOT = Path(__file__).resolve().parents[2]


def test_performance_accounting_and_human_render_are_current() -> None:
    performance.check(ROOT)


def test_contract_projection_does_not_consume_performance_accounting() -> None:
    program = r'''
import hashlib
import importlib.abc
import json
import sys
from pathlib import Path
root = Path.cwd().resolve()
sys.path.insert(0, str(root / "scripts"))
if sys.argv[1] == "deny":
    class BlockPerformance(importlib.abc.MetaPathFinder):
        def find_spec(self, fullname, path=None, target=None):
            if fullname.split(".")[-1] == "performance_objectives":
                raise RuntimeError("contract code imported performance objectives")
    sys.meta_path.insert(0, BlockPerformance())
    protected = (root / "scripts/performance_objectives.py").resolve()
    directory = (root / "qualification/performance").resolve()
    def audit(event, args):
        if event != "open" or not args or not isinstance(args[0], (str, bytes)):
            return
        path = Path(args[0].decode() if isinstance(args[0], bytes) else args[0]).resolve()
        if path == protected or path == directory or directory in path.parents:
            raise RuntimeError("contract code read performance accounting")
    sys.addaudithook(audit)
import contract_freeze
value = contract_freeze.contract_projection()
print(hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest())
'''
    outputs = []
    for mode in ("normal", "deny"):
        completed = subprocess.run([sys.executable, "-c", program, mode], cwd=ROOT,
                                   capture_output=True, text=True, check=True)
        outputs.append(completed.stdout.strip().splitlines()[-1])
    assert outputs[0] == outputs[1]
    assert len(outputs[0]) == 64
