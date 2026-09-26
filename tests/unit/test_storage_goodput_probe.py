from __future__ import annotations

import importlib.util
import itertools
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from uuid import uuid4

import pytest

from scripts import performance_measurement as performance


@pytest.fixture
def probe(monkeypatch: pytest.MonkeyPatch) -> tuple[ModuleType, object]:
    protocol = ModuleType("riverhog_storage_adapter_protocol")
    for name in (
        "DeletePrefixRequest",
        "ObjectLocator",
        "ObjectReadRequest",
        "WriteCompleteRequest",
        "WriteSegmentListRequest",
        "WriteStartRequest",
    ):
        setattr(protocol, name, SimpleNamespace)
    protocol.validate_completed_write_response = lambda *_args: None  # type: ignore[attr-defined]
    support = ModuleType("riverhog_storage_adapter_support")
    support.StorageAdapterClient = SimpleNamespace()  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, protocol.__name__, protocol)
    monkeypatch.setitem(sys.modules, support.__name__, support)
    path = Path(__file__).resolve().parents[1] / "harness/storage_adapter_goodput_probe.py"
    spec = importlib.util.spec_from_file_location("goodput_probe_fixture", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    class Client:
        def __init__(self) -> None:
            self.data = bytearray()
            self.parts: list[int] = []
            self.closed = False
            self.deleted = False
            self.corrupt = False

        def descriptor(self) -> SimpleNamespace:
            return SimpleNamespace(maximum_segment_bytes=4)

        def begin_write(self, _request: object) -> str:
            self.data.clear()
            self.parts.clear()
            return "session"

        def write_segment(self, **kwargs: object) -> None:
            content = b"".join(kwargs["content"])
            assert len(content) == kwargs["stored_bytes"]
            self.data.extend(content)
            self.parts.append(kwargs["number"])

        def list_segments(self, _request: object) -> SimpleNamespace:
            return SimpleNamespace(segments=self.parts, next_after_number=None, completion="done")

        def complete_write(self, request: SimpleNamespace) -> SimpleNamespace:
            assert request.expected_bytes == len(self.data)
            return SimpleNamespace(object_path="fixture/payload", revision="revision")

        def read_object(self, _request: object) -> object:
            data = b"corrupt" if self.corrupt else bytes(self.data)

            class Stream:
                def __enter__(self) -> SimpleNamespace:
                    return SimpleNamespace(content=[data])

                def __exit__(self, *_args: object) -> bool:
                    return False

            return Stream()

        def delete_prefix(self, _request: object) -> None:
            self.deleted = True

        def close(self) -> None:
            self.closed = True

    client = Client()
    support.StorageAdapterClient.from_token_file = lambda *_args, **_kwargs: client  # type: ignore[attr-defined]
    ticks = itertools.count()
    monkeypatch.setattr(module.time, "perf_counter", lambda: float(next(ticks)))
    return module, client


def context() -> dict[str, object]:
    return {
        "format": performance.CONTEXT_FORMAT,
        "comparison_id": str(uuid4()),
        "workload_sha256": "a" * 64,
        "environment_sha256": "b" * 64,
        "path_sha256": "c" * 64,
        "byte_domain": "stored-payload",
        "completion_boundary": "adapter-complete-and-verified-readback",
        "cache_state": "read-after-write",
        "concurrency": 1,
    }


def test_probe_without_reference_reports_absolute_rates_but_no_pass(
    probe: tuple[ModuleType, object], tmp_path: Path
) -> None:
    module, client = probe
    result = module.run(base_url="http://fixture", token_file=tmp_path / "token", payload_bytes=9)
    assert result["format"] == "riverhog-storage-adapter-goodput/v3"
    assert all(item["status"] == "not-compared" for item in result["comparisons"].values())
    assert result["samples"]["upload"]["elapsed_seconds"] == 3
    assert result["samples"]["read"]["elapsed_seconds"] == 1
    assert result["upload_mib_per_second"] > 0
    assert result["read_mib_per_second"] > 0
    assert client.closed and client.deleted
    assert "utilization" not in repr(result)


def test_probe_compares_only_matching_measured_direction(
    probe: tuple[ModuleType, object], tmp_path: Path
) -> None:
    module, _ = probe
    kwargs = {
        "base_url": "http://fixture",
        "token_file": tmp_path / "token",
        "payload_bytes": 9,
        "context": context(),
    }
    reference = module.run(**kwargs)
    measured = module.run(**kwargs, reference=reference, target_ratio=0.9)
    assert all(item["status"] == "met" for item in measured["comparisons"].values())
    reference["samples"]["upload"] = reference["samples"]["read"]
    mismatched = module.run(**kwargs, reference=reference, target_ratio=0.9)
    assert mismatched["comparisons"]["upload"]["status"] == "not-compared"


def test_probe_corruption_is_failure_not_goodput_miss(
    probe: tuple[ModuleType, object], tmp_path: Path
) -> None:
    module, client = probe
    client.corrupt = True
    with pytest.raises(RuntimeError, match="changed the payload"):
        module.run(base_url="http://fixture", token_file=tmp_path / "token", payload_bytes=9)
    assert client.closed and client.deleted


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("byte_domain", "logical-payload"),
        ("cache_state", "cold"),
        ("concurrency", 2),
        ("completion_boundary", "unverified"),
    ],
)
def test_probe_rejects_incompatible_context(
    probe: tuple[ModuleType, object], tmp_path: Path, field: str, value: object
) -> None:
    module, _ = probe
    declared = context()
    declared[field] = value
    with pytest.raises(performance.PerformanceError):
        module.run(
            base_url="http://fixture",
            token_file=tmp_path / "token",
            payload_bytes=9,
            context=declared,
        )
