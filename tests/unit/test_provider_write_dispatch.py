from __future__ import annotations

import subprocess
import sys


def test_provider_dispatch_imports_without_persistent_or_service_owners():
    code = """
import importlib.abc
import sys

class IndependentDispatch(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "sqlalchemy" or fullname.startswith(
            ("sqlalchemy.", "riverhog_core.catalog", "riverhog_core.services")
        ):
            raise AssertionError("dispatch imported a persistent owner")

sys.meta_path.insert(0, IndependentDispatch())
from riverhog_core.provider_write_dispatch import ArmedProviderWrite
from riverhog_protocol.errors import Conflict

ticket = ArmedProviderWrite(
    call_id="a" * 64, collection_id=1, object_id="object", nonce="private-nonce",
    request_sha256="b" * 64, executor_id="c" * 64, target_incarnation="target",
)
assert ticket.dispatch(lambda: "completed") == "completed"
try:
    ticket.dispatch(lambda: "repeated")
except Conflict:
    pass
else:
    raise AssertionError("one-use dispatch was reconstructed")
"""
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
