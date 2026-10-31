"""Binary qualification precedes every process-global heap mutation."""

from __future__ import annotations

import hashlib
import sys
from types import SimpleNamespace

import pytest
import riverhog_client
from riverhog_client import _prepared_primary_runtime as runtime


@pytest.fixture
def loaded(tmp_path, monkeypatch):
    image = tmp_path / "sqlite3.dll"
    image.write_bytes(b"qualified fixture" * 10001)
    expected = hashlib.sha256(image.read_bytes()).hexdigest()
    events = []
    identity = {
        "sqlite_source_id": runtime._SQLITE_SOURCE_ID,
        "sqlite_library_path": str(image),
    }

    def identify():
        events.append("identity")
        return dict(identity)

    def admit(source, limit, native_limits):
        events.append(("heap", source, limit, native_limits))
        return "admitted"

    native = SimpleNamespace(SqliteProcessBudget=admit)
    monkeypatch.setattr(runtime, "_loaded_runtime_identity", identify)
    monkeypatch.setattr(runtime, "_SQLITE_LIBRARY_SHA256", expected)
    monkeypatch.setitem(sys.modules, "riverhog_client._prepared_primary_native", native)
    monkeypatch.setattr(riverhog_client, "_prepared_primary_native", native, raising=False)
    return image, identity, expected, events


def test_qualified_binary_is_checked_before_the_explicit_heap_grant(loaded):
    _image, identity, expected, events = loaded
    assert runtime._qualified_runtime() == {
        "sqlite_source_id": identity["sqlite_source_id"],
        "sqlite_library_sha256": expected,
    }
    assert events == ["identity"]
    assert runtime._admit_process_budget(1048576, native_limits=(128, 512)) == "admitted"
    assert events == [
        "identity",
        "identity",
        ("heap", identity["sqlite_source_id"], 1048576, (128, 512)),
    ]


@pytest.mark.parametrize("failure", ["source", "binary", "missing", "oversize"])
def test_failed_runtime_admission_leaves_the_heap_policy_untouched(loaded, monkeypatch, failure):
    image, identity, _expected, events = loaded
    if failure == "source":
        identity["sqlite_source_id"] = "unqualified source"
        image.unlink()  # Source rejection must precede even opening the image.
        error, message = RuntimeError, "source"
    elif failure == "binary":
        image.write_bytes(b"a different engine with the same source ID")
        error, message = RuntimeError, "binary"
    elif failure == "missing":
        image.unlink()
        error, message = FileNotFoundError, None
    else:
        monkeypatch.setattr(runtime, "_MAX_LIBRARY_BYTES", runtime._LIBRARY_BLOCK_BYTES)
        error, message = RuntimeError, "image envelope"
    with pytest.raises(error, match=message):
        runtime._admit_process_budget(1048576, native_limits=(128, 512))
    assert events == ["identity"]


def test_binary_reads_have_a_fixed_buffer_and_stop_at_the_startup_envelope(loaded, monkeypatch):
    image, _identity, _expected, events = loaded
    original_open = type(image).open
    reads = []

    class Image:
        def __init__(self, stream):
            self.stream = stream

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.stream.close()

        def read(self, amount):
            reads.append(amount)
            return self.stream.read(amount)

    def open_image(path, *args, **kwargs):
        return Image(original_open(path, *args, **kwargs))

    monkeypatch.setattr(type(image), "open", open_image)
    monkeypatch.setattr(runtime, "_MAX_LIBRARY_BYTES", runtime._LIBRARY_BLOCK_BYTES)
    with pytest.raises(RuntimeError, match="image envelope"):
        runtime._admit_process_budget(1048576, native_limits=(128, 512))
    assert reads == [runtime._LIBRARY_BLOCK_BYTES] * 2
    assert events == ["identity"]


def test_loaded_module_probe_uses_stdlib_and_the_private_native_api(monkeypatch):
    identity = {"sqlite_source_id": "loaded", "sqlite_library_path": "already-loaded"}
    native = SimpleNamespace(runtime_identity=lambda: identity)
    monkeypatch.setitem(sys.modules, "riverhog_client._prepared_primary_native", native)
    monkeypatch.setattr(riverhog_client, "_prepared_primary_native", native, raising=False)
    assert runtime._loaded_runtime_identity() is identity


def test_application_native_grants_are_required_before_loading_the_runtime(loaded):
    _image, _identity, _expected, events = loaded
    with pytest.raises(TypeError):
        runtime._admit_process_budget(1048576)
    assert events == []


def test_explicit_zero_population_grants_are_forwarded_without_substitution(loaded):
    _image, identity, _expected, events = loaded
    assert runtime._admit_process_budget(1048576, native_limits=(0, 0)) == "admitted"
    assert events == ["identity", ("heap", identity["sqlite_source_id"], 1048576, (0, 0))]
