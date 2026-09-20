from __future__ import annotations

import importlib.util
import itertools
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from uuid import uuid4

import pytest

from scripts import performance_objectives as p


@pytest.fixture
def probe(monkeypatch):
    # Exercise the real probe logic with a controlled protocol fixture, not live storage.
    protocol = ModuleType("riverhog_storage_adapter_protocol")
    for name in ("DeletePrefixRequest", "ObjectLocator", "ObjectReadRequest",
        "WriteCompleteRequest",
                 "WriteSegmentListRequest", "WriteStartRequest"):
        setattr(protocol, name, SimpleNamespace)
    protocol.validate_completed_write_response = lambda *_args: None
    support = ModuleType("riverhog_storage_adapter_support")
    support.StorageAdapterClient = SimpleNamespace()
    monkeypatch.setitem(sys.modules, protocol.__name__, protocol)
    monkeypatch.setitem(sys.modules, support.__name__, support)
    path = Path(__file__).resolve().parents[1] / "harness/storage_adapter_goodput_probe.py"
    spec = importlib.util.spec_from_file_location("probe_fixture", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    class Client:
        def __init__(self):
            self.data = bytearray()
            self.parts = []
            self.closed = False
            self.deleted = False
            self.corrupt = False
        def descriptor(self):
            return SimpleNamespace(maximum_segment_bytes=4)
        def begin_write(self, request):
            self.data.clear()
            self.parts.clear()
            return "session"
        def write_segment(self, **kwargs):
            content = b"".join(kwargs["content"])
            assert len(content) == kwargs["stored_bytes"]
            self.data.extend(content)
            self.parts.append(kwargs["number"])
        def list_segments(self, request):
            return SimpleNamespace(segments=self.parts, next_after_number=None,
                completion="complete")
        def complete_write(self, request):
            assert request.expected_bytes == len(self.data)
            return SimpleNamespace(object_path="fixture/payload", revision="revision")
        def read_object(self, request):
            data = b"corrupt" if self.corrupt else bytes(self.data)
            class Stream:
                def __enter__(self):
                    return SimpleNamespace(content=[data])
                def __exit__(self, *_args):
                    return False
            return Stream()
        def delete_prefix(self, request):
            self.deleted = True
        def close(self):
            self.closed = True
    client = Client()
    support.StorageAdapterClient.from_token_file = lambda *_args, **_kwargs: client
    monkeypatch.setattr(p, "source_identity", lambda: {"sha": "1" * 40, "clean": True})
    ticks = itertools.count()
    monkeypatch.setattr(module.time, "perf_counter", lambda: float(next(ticks)))
    return module, client


def context():
    return {"schema": p.CONTEXT_SCHEMA, "comparison_id": str(uuid4()),
            "workload_sha256": "a" * 64, "environment_sha256": "b" * 64, "path_sha256": "c" * 64,
            "byte_domain": "stored-payload",
        "completion_boundary": "adapter-complete-and-verified-readback",
            "cache_state": "read-after-write", "concurrency": 1}


def test_probe_without_baseline_reports_but_does_not_pass_objective(probe, tmp_path):
    module, client = probe
    result = module.run(base_url="http://fixture", token_file=tmp_path / "token", payload_bytes=9)
    assert all(row["status"] == "not_evaluated" for row in result["evaluations"].values())
    assert result["samples"]["upload"]["elapsed_seconds"] == 3
    assert result["samples"]["read"]["elapsed_seconds"] == 1
    assert client.closed and client.deleted
    assert "utilization" not in repr(result)


def test_probe_uses_separate_matched_directional_results(probe, tmp_path):
    module, _ = probe
    kwargs = dict(base_url="http://fixture", token_file=tmp_path / "token", payload_bytes=9,
        context=context())
    baseline = module.run(**kwargs)
    result = module.run(**kwargs, reference=baseline)
    assert all(row["status"] == "met" for row in result["evaluations"].values())
    baseline["samples"]["upload"] = baseline["samples"]["read"]
    result = module.run(**kwargs, reference=baseline)
    assert result["evaluations"]["upload"]["status"] == "not_evaluated"


def test_corruption_is_a_failure_not_a_goodput_miss(probe, tmp_path):
    module, client = probe
    client.corrupt = True
    with pytest.raises(RuntimeError, match="changed the payload"):
        module.run(base_url="http://fixture", token_file=tmp_path / "token", payload_bytes=9)
    assert client.closed and client.deleted


def test_probe_rejects_wrong_byte_domain_or_cache_state(probe, tmp_path):
    module, _ = probe
    for key, value in (("byte_domain", "logical-payload"), ("cache_state", "cold"),
        ("concurrency", 2)):
        ctx = context()
        ctx[key] = value
        with pytest.raises(p.PerformanceError):
            module.run(base_url="http://fixture", token_file=tmp_path / "token",
                payload_bytes=9, context=ctx)
