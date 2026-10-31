from __future__ import annotations

import hashlib
import io
import json
import os
import subprocess
import sys
import weakref
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from pathlib import Path
from threading import Event
from types import SimpleNamespace

import pytest
from a_riverhog_cli import directory_upload, upload_identity_records, upload_sources
from a_riverhog_cli.upload_identity import initialize_upload_identity, open_upload_identity
from riverhog_canonical_json import canonical_json_bytes
from riverhog_client import PrimaryStateCapacity, PrimaryStateError

from tests.support.cli_upload import read_upload_sources, verified_upload_sources
from tests.support.prepared_primary import SqlitePrimaryTestGuard


def identity_document(*args):
    document = directory_upload._new_upload_identity(*args)
    document["sources"] = list(document["sources"])
    return document


def identity_body(storage, *, byte_limit):
    with open_upload_identity(storage, byte_limit=byte_limit) as stream:
        return stream.read()


def candidate_body(storage, body):
    from hashlib import sha256

    from riverhog_client import PrimaryByteFile

    digest = sha256(body).hexdigest()
    candidate = PrimaryByteFile("fixture-stage-" + digest, len(body), digest)
    if not storage.exists("upload-identity") and not storage.exists("source-identity"):
        if not storage.exists(candidate.name):
            assert (
                storage.put_bytes(candidate.name, (body,), byte_limit=max(1, len(body)))
                == candidate
            )
    return candidate


def initialized_body(storage, body, **kwargs):
    with initialize_upload_identity(storage, candidate_body(storage, body), **kwargs) as stream:
        return stream.read()


@contextmanager
def opened(path, backend, *, initialize):
    if backend == "posix":
        from riverhog_client._prepared_primary_factory import posix_primary_files

        with posix_primary_files(
            path, allocation_descriptor=b"application identity", initialize=initialize
        ) as storage:
            yield storage
    else:
        from riverhog_client._prepared_primary_sqlite import (
            SqlitePrimaryLimits,
            TransactionalPrimaryFiles,
        )

        if initialize:
            path.touch(exist_ok=False)
        with TransactionalPrimaryFiles(
            SqlitePrimaryTestGuard(path),
            allocation_descriptor=b"application identity",
            limits=SqlitePrimaryLimits(16 * 1024**2, 32 * 1024**2, 128 * 1024),
            initialize=initialize,
        ) as storage:
            yield storage


@pytest.mark.parametrize("changed", ["none", "key", "source", "duplicate-id", "view"])
def test_identity_codec_preserves_original_allocation_without_generating_replacements(
    tmp_path, monkeypatch, changed
):
    root = tmp_path / "source"
    root.mkdir()
    (root / "a").write_bytes(b"original a")
    (root / "b").write_bytes(b"original b")
    root, current = verified_upload_sources(root, "retained")
    original = identity_document(root, "retained", current)
    exact = canonical_json_bytes(original)
    loaded = json.loads(exact)
    key = "retained"
    if changed == "key":
        key = "other"
    elif changed == "source":
        current = [dict(item) for item in current]
        current[0]["sha256"] = "0" * 64
    elif changed == "duplicate-id":
        loaded["sources"][1]["artifact_id"] = loaded["sources"][0]["artifact_id"]
    elif changed == "view":
        loaded["source_naming_view_id"] = "invalid"

    def forbidden(*args, **kwargs):
        pytest.fail("identity replay generated replacement identities")

    monkeypatch.setattr(directory_upload.secrets, "token_hex", forbidden)
    if changed == "none":
        upload = directory_upload._load_upload_identity(
            io.BytesIO(canonical_json_bytes(loaded)),
            root,
            key,
            current,
            open_original=lambda: io.BytesIO(canonical_json_bytes(loaded)),
        )
        assert tuple(source.artifact_id for source in read_upload_sources(upload)) == tuple(
            source["artifact_id"] for source in original["sources"]
        )
        assert upload.source_naming_view_id == original["source_naming_view_id"]
    else:
        with pytest.raises(ValueError):
            directory_upload._load_upload_identity(
                io.BytesIO(canonical_json_bytes(loaded)),
                root,
                key,
                current,
                open_original=lambda: io.BytesIO(canonical_json_bytes(loaded)),
            )
    assert canonical_json_bytes(original) == exact


@pytest.mark.skipif(os.name != "posix", reason="actual POSIX and Unix-VFS identity adapters")
@pytest.mark.parametrize("backend", ["posix", "sqlite"])
def test_storage_identity_commits_original_reference_before_payload_and_reopens_exactly(
    tmp_path, backend
):
    path = tmp_path / ("directory" if backend == "posix" else "identity.sqlite3")
    body = b'{"original":"opaque application bytes"}' * 200
    events = []
    with opened(path, backend, initialize=True) as storage:

        def prepared():
            reference = json.loads(storage.read_control("upload-identity", byte_limit=1024))
            assert reference["bytes"] == str(len(body))
            assert reference["sha256"] == hashlib.sha256(body).hexdigest()
            assert not storage.exists("source-identity")
            events.append("prepared")

        assert (
            initialized_body(
                storage, body, byte_limit=len(body), control_capacity=1024, prepare_payload=prepared
            )
            == body
        )
        assert events and set(events) == {"prepared"}
        exact = storage.read_control("upload-identity", byte_limit=1024)
    with opened(path, backend, initialize=False) as storage:
        assert identity_body(storage, byte_limit=len(body)) == body
        assert storage.read_control("upload-identity", byte_limit=1024) == exact
        with pytest.raises(FileExistsError, match="conflicts with retained identity"):
            initialized_body(
                storage,
                b"replacement",
                byte_limit=len(body),
                control_capacity=1024,
                prepare_payload=lambda: None,
            )
        assert identity_body(storage, byte_limit=len(body)) == body


@pytest.mark.skipif(os.name != "posix", reason="actual POSIX and Unix-VFS identity adapters")
@pytest.mark.parametrize("backend", ["posix", "sqlite"])
def test_identity_capacity_refusal_does_not_prepare_or_publish_state(tmp_path, backend):
    path = tmp_path / ("directory" if backend == "posix" else "identity.sqlite3")
    with opened(path, backend, initialize=True) as storage:

        def forbidden():
            pytest.fail("unadmitted identity reached preparation")

        with pytest.raises(PrimaryStateCapacity):
            initialized_body(
                storage, b"original", byte_limit=7, control_capacity=1024, prepare_payload=forbidden
            )
        assert not storage.exists("upload-identity") and not storage.exists("source-identity")


@pytest.mark.skipif(os.name != "posix", reason="actual POSIX and Unix-VFS identity adapters")
@pytest.mark.parametrize("backend", ["posix", "sqlite"])
def test_preparation_failure_keeps_reference_and_forbids_replacement_on_resume(tmp_path, backend):
    path = tmp_path / ("directory" if backend == "posix" else "identity.sqlite3")

    def unavailable():
        raise OSError("physical preparation not confirmed")

    with opened(path, backend, initialize=True) as storage:
        with pytest.raises(OSError, match="preparation not confirmed"):
            initialized_body(
                storage,
                b"original",
                byte_limit=64,
                control_capacity=1024,
                prepare_payload=unavailable,
            )
        exact = storage.read_control("upload-identity", byte_limit=1024)
        assert not storage.exists("source-identity")
    with opened(path, backend, initialize=False) as storage:
        with pytest.raises(FileNotFoundError):
            identity_body(storage, byte_limit=64)
        with pytest.raises(FileExistsError):
            initialized_body(
                storage,
                b"different",
                byte_limit=64,
                control_capacity=1024,
                prepare_payload=lambda: None,
            )
        assert storage.read_control("upload-identity", byte_limit=1024) == exact


@pytest.mark.skipif(os.name != "posix", reason="actual POSIX and Unix-VFS identity adapters")
@pytest.mark.parametrize("backend", ["posix", "sqlite"])
@pytest.mark.parametrize("missing", ["reference", "content", "invalid-reference"])
def test_missing_or_invalid_identity_half_never_reconstructs_authority(tmp_path, backend, missing):
    path = tmp_path / ("directory" if backend == "posix" else "identity.sqlite3")
    with opened(path, backend, initialize=True) as storage:
        initialized_body(
            storage, b"original", byte_limit=64, control_capacity=1024, prepare_payload=lambda: None
        )
        exact = storage.read_control("upload-identity", byte_limit=1024)
        if missing == "reference":
            storage.remove_control("upload-identity", expected=exact)
        elif missing == "invalid-reference":
            storage.put_control("upload-identity", b"{}", byte_limit=1024, expected=exact)
        else:
            for _ in range(16):
                if storage.remove_step("source-identity"):
                    break
            else:
                pytest.fail("fixture identity cleanup did not finish")
    with opened(path, backend, initialize=False) as storage:
        with pytest.raises((FileNotFoundError, PrimaryStateError)):
            identity_body(storage, byte_limit=64)
        with pytest.raises(FileExistsError):
            initialized_body(
                storage,
                b"replacement",
                byte_limit=64,
                control_capacity=1024,
                prepare_payload=lambda: None,
            )


@pytest.mark.skipif(os.name != "posix", reason="actual POSIX and Unix-VFS identity adapters")
@pytest.mark.parametrize("backend", ["posix", "sqlite"])
def test_smaller_replay_admission_keeps_original_identity_for_larger_offer(tmp_path, backend):
    path = tmp_path / ("directory" if backend == "posix" else "identity.sqlite3")
    with opened(path, backend, initialize=True) as storage:
        initialized_body(
            storage, b"original", byte_limit=64, control_capacity=1024, prepare_payload=lambda: None
        )
        exact = storage.read_control("upload-identity", byte_limit=1024)
        with pytest.raises(PrimaryStateCapacity):
            identity_body(storage, byte_limit=7)
        assert storage.read_control("upload-identity", byte_limit=1024) == exact
        assert identity_body(storage, byte_limit=64) == b"original"


@pytest.mark.skipif(os.name != "posix", reason="actual POSIX and Unix-VFS identity adapters")
@pytest.mark.parametrize("backend", ["posix", "sqlite"])
def test_concurrent_initialization_cannot_replace_the_first_committed_identity(tmp_path, backend):
    path = tmp_path / ("directory" if backend == "posix" else "identity.sqlite3")
    entered, release, second_started = Event(), Event(), Event()
    with opened(path, backend, initialize=True) as storage:

        def prepare():
            entered.set()
            assert release.wait(5)

        def competing():
            second_started.set()
            return initialized_body(
                storage,
                b"replacement",
                byte_limit=64,
                control_capacity=1024,
                prepare_payload=lambda: None,
            )

        with ThreadPoolExecutor(max_workers=2) as workers:
            first = workers.submit(
                initialized_body,
                storage,
                b"original",
                byte_limit=64,
                control_capacity=1024,
                prepare_payload=prepare,
            )
            try:
                assert entered.wait(5)
                second = workers.submit(competing)
                assert second_started.wait(5)
            finally:
                release.set()
            assert first.result(5) == b"original"
            with pytest.raises(FileExistsError):
                second.result(5)
        exact = storage.read_control("upload-identity", byte_limit=1024)
        assert identity_body(storage, byte_limit=64) == b"original"
    with opened(path, backend, initialize=False) as storage:
        assert storage.read_control("upload-identity", byte_limit=1024) == exact
        assert identity_body(storage, byte_limit=64) == b"original"


@pytest.mark.skipif(os.name != "nt", reason="qualified Windows engine and private local NTFS")
def test_native_windows_upload_identity_keeps_exact_ids_and_never_reinitializes_missing_state(
    tmp_path,
):
    code = r"""
def read_upload_sources(upload):
    with upload.open_sources() as sources:
        return tuple(sources)

import importlib.util,io,json,sys
from pathlib import Path
root=Path(sys.argv[1]);sys.path.insert(0,str(root))
from a_riverhog_cli import directory_upload
from a_riverhog_cli.upload_identity import (
    initialize_upload_identity,open_upload_identity,stage_upload_identity)
def identity_body(storage, *, byte_limit):
    with open_upload_identity(storage, byte_limit=byte_limit) as stream:
        return stream.read()


def candidate_body(storage, body):
    from riverhog_client import PrimaryByteFile

    from hashlib import sha256

    digest = sha256(body).hexdigest()
    candidate = PrimaryByteFile("fixture-stage-" + digest, len(body), digest)
    if not storage.exists("upload-identity") and not storage.exists("source-identity"):
        if not storage.exists(candidate.name):
            assert storage.put_bytes(
                candidate.name, (body,), byte_limit=max(1, len(body))
            ) == candidate
    return candidate


def initialized_body(storage, body, **kwargs):
    with initialize_upload_identity(storage, candidate_body(storage, body), **kwargs) as stream:
        return stream.read()



from a_riverhog_cli.upload_sqlite import UploadSqlitePolicy,UploadSqliteResources
from riverhog_canonical_json import canonical_json_bytes
from riverhog_client import (
    WindowsSqliteProcess,SqlitePrimaryLimits,SqliteOperationBank,SqliteOperationGrants,
    SqliteReadinessPlan,sqlite_primary_preparation_step)
spec=importlib.util.spec_from_file_location("identity_parent",
    root/"tests/platform/windows_primary_process.py")
parents=importlib.util.module_from_spec(spec);sys.modules[spec.name]=parents
spec.loader.exec_module(parents)
parent=Path(sys.argv[2])/"private-parent";parents._parent(parent)
source=Path(sys.argv[2])/"source";source.mkdir();(source/"member").write_bytes(b"original")
from tests.support.cli_upload import verified_upload_sources
source,current=verified_upload_sources(source,"retained")
document=directory_upload._new_upload_identity(source,"retained",current)
document["sources"]=list(document["sources"])
body=canonical_json_bytes(document)
process=WindowsSqliteProcess(heap_bytes=64*1024**2,native_limits=(1,2))
resources=UploadSqliteResources(UploadSqlitePolicy(connections=1,blobs=1),process=process)
descriptor=b"explicit application identity allocation"
path=process.primary_location(parent,descriptor)
limits=SqlitePrimaryLimits(16*1024**2,32*1024**2,128*1024)
bank=SqliteOperationBank(1024,8*1024**2,65536,8*1024**2,65536,
    262144,0,1<<40,1<<60,1<<40)
factory=process.primary_factory(limits=limits,operations=SqliteOperationGrants(bank,bank),
    retry_profile=(2,3),connection_factory=resources.connect_primary)
plan=SqliteReadinessPlan((1033,16),65536,8,128,1024*1024)
try:
    with factory(path,allocation_descriptor=descriptor,initialize=True) as storage:
        identity=storage.child("directory-upload",initialize=True)
        def prepare():
            for _ in range(512):
                if sqlite_primary_preparation_step(storage,plan=plan):return
            raise AssertionError("identity preparation did not finish")
        staged=stage_upload_identity(
            identity,(body[:13],body[13:]),byte_limit=8192,prepare_payload=prepare)
        with initialize_upload_identity(
            identity,staged,byte_limit=8192,control_capacity=1024,
            prepare_payload=prepare) as original:
            assert original.read()==body
    def forbidden(*args,**kwargs):
        raise AssertionError("resume generated replacement identities")
    directory_upload.secrets.token_hex=forbidden
    with factory(path,allocation_descriptor=descriptor,initialize=False) as storage:
        identity=storage.child("directory-upload",initialize=False)
        assert identity_body(identity,byte_limit=8192)==body
        with open_upload_identity(identity,byte_limit=8192) as replay:
            assert replay.read(7)==body[:7]
            assert replay.read()==body[7:]
        assert replay.closed
        loaded=directory_upload._load_upload_identity(
            io.BytesIO(body),source,"retained",current,open_original=lambda:io.BytesIO(body))
        assert read_upload_sources(loaded)[0].artifact_id==document["sources"][0]["artifact_id"]
        assert loaded.source_naming_view_id==document["source_naming_view_id"]
        exact=identity.read_control("upload-identity",byte_limit=1024)
        identity.remove_control("upload-identity",expected=exact)
    with factory(path,allocation_descriptor=descriptor,initialize=False) as storage:
        identity=storage.child("directory-upload",initialize=False)
        try:identity_body(identity,byte_limit=8192)
        except FileNotFoundError:pass
        else:raise AssertionError("missing original reference was reconstructed")
        try:initialized_body(
            identity,b"replacement",byte_limit=8192,control_capacity=1024,
            prepare_payload=lambda:None)
        except FileExistsError:pass
        else:raise AssertionError("retained original content was replaced")
finally:
    resources.close()
assert path.is_file() and resources.status.connections==resources.status.blobs==0
assert process.native_report()["owners"]==process.native_report()["files"]==0
print(json.dumps({"passed":True,"exact_original_ids":True,"missing_reference_stops":True}))
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", code, str(Path(__file__).resolve().parents[2]), str(tmp_path)],
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "passed": True,
        "exact_original_ids": True,
        "missing_reference_stops": True,
    }


@pytest.mark.skipif(os.name != "posix", reason="actual POSIX and Unix-VFS identity adapters")
@pytest.mark.parametrize("backend", ["posix", "sqlite"])
@pytest.mark.parametrize("control_capacity", [1024, 8192, 65536])
def test_identity_uses_owner_slot_capacity_without_changing_reference_bytes(
    tmp_path, backend, control_capacity
):
    path = tmp_path / ("directory" if backend == "posix" else "identity.sqlite3")
    body = b"original immutable source identity"
    expected = canonical_json_bytes(
        {
            "format": "a-riverhog-cli-source-identity/v1",
            "bytes": str(len(body)),
            "sha256": hashlib.sha256(body).hexdigest(),
        }
    )
    with opened(path, backend, initialize=True) as storage:
        storage.prepare_controls(("unrelated",), byte_limit=control_capacity)
        storage.put_control("unrelated", b"kept", byte_limit=control_capacity)
        with closing(storage.child("directory-upload", initialize=True)) as identity:
            assert (
                initialized_body(
                    identity,
                    body,
                    byte_limit=len(body),
                    control_capacity=control_capacity,
                    prepare_payload=lambda: None,
                )
                == body
            )
            assert identity.read_control("upload-identity", byte_limit=1024) == expected
            assert storage.read_control("unrelated", byte_limit=4) == b"kept"
    with opened(path, backend, initialize=False) as storage:
        with closing(storage.child("directory-upload", initialize=False)) as identity:
            assert identity_body(identity, byte_limit=len(body)) == body
            assert identity.read_control("upload-identity", byte_limit=1024) == expected


@pytest.mark.skipif(os.name != "posix", reason="actual Unix-VFS uniform control capacity")
@pytest.mark.parametrize("offered", [1024, 16384])
def test_identity_cannot_change_the_selected_construction_control_capacity(tmp_path, offered):
    with opened(tmp_path / "identity.sqlite3", "sqlite", initialize=True) as storage:
        storage.prepare_controls(("unrelated",), byte_limit=8192)
        storage.put_control("unrelated", b"kept", byte_limit=8192)
        with closing(storage.child("directory-upload", initialize=True)) as identity:
            with pytest.raises(PrimaryStateError, match="control capacity changed"):
                initialized_body(
                    identity,
                    b"original",
                    byte_limit=64,
                    control_capacity=offered,
                    prepare_payload=lambda: None,
                )
            assert not identity.exists("upload-identity") and (
                not identity.exists("source-identity")
            )
            assert storage.read_control("unrelated", byte_limit=4) == b"kept"


@pytest.mark.skipif(os.name != "posix", reason="actual POSIX and Unix-VFS identity adapters")
@pytest.mark.parametrize("backend", ["posix", "sqlite"])
def test_small_identity_reference_slot_refuses_before_preparation_or_publication(tmp_path, backend):
    path = tmp_path / ("directory" if backend == "posix" else "identity.sqlite3")
    with opened(path, backend, initialize=True) as storage:

        def forbidden():
            pytest.fail("unadmitted reference reached payload preparation")

        with pytest.raises(PrimaryStateCapacity, match="reference exceeds"):
            initialized_body(
                storage, b"original", byte_limit=64, control_capacity=1, prepare_payload=forbidden
            )
        assert not storage.exists("upload-identity") and not storage.exists("source-identity")


@pytest.mark.parametrize("capacity", [True, 0, -1, 1.5])
def test_identity_reference_capacity_is_explicit_before_storage_access(capacity):
    with pytest.raises(ValueError, match="positive byte admission"):
        from riverhog_client import PrimaryByteFile

        with initialize_upload_identity(
            object(),
            PrimaryByteFile("stage", 8, hashlib.sha256(b"original").hexdigest()),
            byte_limit=64,
            control_capacity=capacity,
            prepare_payload=lambda: None,
        ):
            pytest.fail("invalid reference admission reached storage")


@pytest.mark.skipif(os.name != "posix", reason="actual POSIX and Unix-VFS identity adapters")
@pytest.mark.parametrize("backend", ["posix", "sqlite"])
def test_larger_physical_slot_does_not_raise_the_reference_read_bound(tmp_path, backend):
    path = tmp_path / ("directory" if backend == "posix" else "identity.sqlite3")
    with opened(path, backend, initialize=True) as storage:
        initialized_body(
            storage, b"original", byte_limit=64, control_capacity=8192, prepare_payload=lambda: None
        )
        original = storage.read_control("upload-identity", byte_limit=1024)
        oversized = b"x" * 1025
        storage.put_control("upload-identity", oversized, byte_limit=8192, expected=original)
        with pytest.raises(PrimaryStateCapacity):
            identity_body(storage, byte_limit=64)
        storage.put_control("upload-identity", original, byte_limit=8192, expected=oversized)
        assert identity_body(storage, byte_limit=64) == b"original"


@pytest.mark.parametrize("members", [1, 3, 128, 1024])
def test_upload_identity_record_encoding_preserves_canonical_originals(monkeypatch, members):
    current = [
        {
            "relative_parts": [
                'quote-"',
                "backslash-\\",
                "unicode-雪-😀",
                "tab\tnewline\n",
                "long-" + "x" * 512,
                str(index),
            ],
            "bytes": 0 if index % 3 == 0 else 11,
            "sha256": hashlib.sha256(b"" if index % 3 == 0 else b"equal bytes").hexdigest(),
        }
        for index in range(members)
    ]
    original = identity_document(Path("/source"), "record-encoding", current)
    exact = canonical_json_bytes(original)
    before = json.loads(exact)
    serialized = []

    def bounded(value):
        # Reaching the shared codec with the complete collection would retain
        # its recursively prepared copy and serialized bytes at once.
        assert "sources" not in value
        serialized.append(value)
        return canonical_json_bytes(value)

    monkeypatch.setattr(directory_upload, "canonical_json_bytes", bounded)
    encoded = directory_upload._upload_identity_chunks(original)
    first = next(encoded)
    assert len(serialized) == 1
    assert b"".join((first, *encoded)) == exact
    assert len(serialized) == members + 1
    assert original == before
    assert len({row["artifact_id"] for row in original["sources"]}) == members


@pytest.mark.parametrize("changed", ["missing", "extra"])
def test_upload_identity_record_encoding_rejects_unknown_document_shapes(changed):
    original = identity_document(Path("/source"), "record-encoding", [])
    if changed == "missing":
        original.pop("root")
    else:
        original["unrecorded"] = "must not be silently omitted"
    with pytest.raises(ValueError, match="complete known document"):
        next(directory_upload._upload_identity_chunks(original))


@pytest.mark.skipif(os.name != "posix", reason="POSIX link and directory fsync publication")
@pytest.mark.parametrize("failure", ["none", "record", "file-sync", "link", "directory-sync"])
def test_record_identity_publication_keeps_only_a_complete_selected_original(
    tmp_path, monkeypatch, failure
):
    root = tmp_path / "sources"
    root.mkdir()
    (root / "empty").write_bytes(b"")
    (root / "one").write_bytes(b"equal bytes")
    (root / "two").write_bytes(b"equal bytes")
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    root, current = verified_upload_sources(root, "retained")
    original = identity_document(root, "retained", current)
    exact = canonical_json_bytes(original)
    monkeypatch.setattr(directory_upload, "_new_upload_identity", lambda *args: original)
    destination = directory_upload._state_path(root, "retained")
    real_chunks = directory_upload._upload_identity_chunks
    real_sync = directory_upload.os.fsync

    def failed_chunks(document):
        for index, chunk in enumerate(real_chunks(document)):
            if index == 2:
                raise OSError("injected record interruption")
            yield chunk

    sync_calls = []

    def failed_sync(descriptor):
        sync_calls.append(descriptor)
        if len(sync_calls) == (1 if failure == "file-sync" else 2):
            raise OSError("injected sync interruption")
        return real_sync(descriptor)

    def failed_link(*args, **kwargs):
        raise OSError("injected link interruption")

    with monkeypatch.context() as faults:
        if failure == "record":
            faults.setattr(directory_upload, "_upload_identity_chunks", failed_chunks)
        elif failure == "link":
            faults.setattr(directory_upload.os, "link", failed_link)
        elif failure != "none":
            faults.setattr(directory_upload.os, "fsync", failed_sync)
        if failure == "none":
            directory_upload._prepare_verified_upload(root, "retained", current)
        else:
            with pytest.raises(OSError, match="injected"):
                directory_upload._prepare_verified_upload(root, "retained", current)
    assert not list(destination.parent.glob(".upload-*"))
    if failure not in ("none", "directory-sync"):
        assert not destination.exists()
        return
    assert destination.read_bytes() == exact

    def forbidden(*args, **kwargs):
        pytest.fail("cold replay generated replacement identities")

    monkeypatch.setattr(directory_upload, "_new_upload_identity", forbidden)
    monkeypatch.setattr(directory_upload, "_upload_identity_chunks", forbidden)
    restored = directory_upload._prepare_verified_upload(
        root, "retained", current, initialize=False
    )
    assert [source.artifact_id for source in read_upload_sources(restored)] == [
        row["artifact_id"] for row in original["sources"]
    ]
    assert restored.source_naming_view_id == original["source_naming_view_id"]
    assert [source.bytes for source in read_upload_sources(restored)] == [0, 11, 11]
    assert destination.read_bytes() == exact


def _replay_original(tmp_path, members=3):
    root = tmp_path / "source"
    (root / "nested").mkdir(parents=True)
    for index in range(members):
        (root / "nested" / f"member-{index}-雪.bin").write_bytes(
            b"" if index == 0 else b"equal bytes"
        )
    root, current = verified_upload_sources(root, "retained")
    original = identity_document(root, "retained", current)
    return root, current, original


@pytest.mark.parametrize("members", [1, 3])
def test_retained_source_descriptors_preserve_original_paths_and_producer_inputs(
    tmp_path, monkeypatch, members
):
    root, current, original = _replay_original(tmp_path, members)
    original["sources"][0]["opaque-extra"] = {"uninterpreted": [1, 2]}
    exact = canonical_json_bytes(original)
    current_exact = canonical_json_bytes(current)

    def forbidden(*args, **kwargs):
        pytest.fail("retained source replay allocated replacement identities")

    monkeypatch.setattr(directory_upload.secrets, "token_hex", forbidden)
    monkeypatch.setattr(directory_upload.uuid, "uuid4", forbidden)
    upload = directory_upload._load_upload_identity(
        io.BytesIO(canonical_json_bytes(original)),
        root,
        "retained",
        current,
        open_original=lambda: io.BytesIO(canonical_json_bytes(original)),
    )
    assert upload.source_naming_view_id == original["source_naming_view_id"]
    assert len(read_upload_sources(upload)) == members
    for source, recorded in zip(read_upload_sources(upload), original["sources"], strict=True):
        expected_path = root.joinpath(*recorded["relative_parts"])
        assert source.path == expected_path
        assert source.relative_parts == tuple(recorded["relative_parts"])
        assert (source.artifact_id, source.bytes, source.sha256) == (
            recorded["artifact_id"],
            recorded["bytes"],
            recorded["sha256"],
        )
        consumed = source.producer_file()
        assert consumed.source == expected_path
        assert consumed.artifact_id == recorded["artifact_id"]
        assert consumed.materialization_hint == tuple(recorded["relative_parts"])
        assert consumed.allow_missing_materialization_hint is False
    assert canonical_json_bytes(original) == exact
    assert canonical_json_bytes(current) == current_exact


@pytest.mark.parametrize(
    "changed",
    [
        "fewer",
        "extra",
        "different-source",
        "non-object-source",
        "missing-id",
        "short-id",
        "upper-id",
        "nonhex-id",
        "null-id",
        "list-id",
        "object-id",
        "duplicate-last-id",
        "view",
    ],
)
def test_retained_source_preflight_finishes_before_descriptor_materialization(
    tmp_path, monkeypatch, changed
):
    root, current, original = _replay_original(tmp_path)
    if changed == "fewer":
        original["sources"].pop()
    elif changed == "extra":
        original["sources"].append(dict(original["sources"][-1]))
    elif changed == "different-source":
        original["sources"][-1]["sha256"] = "0" * 64
    elif changed == "non-object-source":
        original["sources"][-1] = None
    elif changed == "missing-id":
        original["sources"][-1].pop("artifact_id")
    elif changed == "view":
        original["source_naming_view_id"] = "invalid"
    else:
        replacement = {
            "short-id": "a" * 63,
            "upper-id": "A" * 64,
            "nonhex-id": "g" * 64,
            "null-id": None,
            "list-id": [],
            "object-id": {},
            "duplicate-last-id": original["sources"][0]["artifact_id"],
        }[changed]
        original["sources"][-1]["artifact_id"] = replacement
    exact = canonical_json_bytes(original)
    current_exact = canonical_json_bytes(current)

    def forbidden(*args, **kwargs):
        pytest.fail("invalid retained identity reached descriptor creation or new IDs")

    monkeypatch.setattr(upload_sources, "LocalSource", forbidden)
    monkeypatch.setattr(directory_upload.secrets, "token_hex", forbidden)
    monkeypatch.setattr(directory_upload.uuid, "uuid4", forbidden)
    with pytest.raises((ValueError, TypeError, AttributeError)):
        directory_upload._load_upload_identity(
            io.BytesIO(canonical_json_bytes(original)),
            root,
            "retained",
            current,
            open_original=lambda: io.BytesIO(canonical_json_bytes(original)),
        )
    assert canonical_json_bytes(original) == exact
    assert canonical_json_bytes(current) == current_exact


@pytest.mark.skipif(os.name != "posix", reason="actual POSIX and Unix-VFS identity adapters")
@pytest.mark.parametrize("backend", ["posix", "sqlite"])
@pytest.mark.parametrize("initialize", [True, False])
@pytest.mark.parametrize("interrupted", [False, True])
def test_identity_stream_has_no_implicit_consumer_read_and_closes_before_reuse(
    tmp_path, monkeypatch, backend, initialize, interrupted
):
    path = tmp_path / ("directory" if backend == "posix" else "identity.sqlite3")
    body = b"original opaque identity " * 257
    with opened(path, backend, initialize=True) as storage:
        if not initialize:
            assert (
                initialized_body(
                    storage,
                    body,
                    byte_limit=len(body),
                    control_capacity=1024,
                    prepare_payload=lambda: None,
                )
                == body
            )
        candidate = candidate_body(storage, body) if initialize else None
        verified = storage.verified
        reads = []
        retained = []

        class ConsumerFailure(RuntimeError):
            pass

        class ConsumerWindow:
            def __init__(self, actual):
                self.actual = actual

            @property
            def closed(self):
                return self.actual.closed

            def read(self, size=-1):
                assert type(size) is int and 0 < size <= 31
                reads.append(size)
                return self.actual.read(size)

        @contextmanager
        def guarded(reference):
            with verified(reference) as actual:
                if reference.name != "source-identity":
                    yield actual
                else:
                    retained.append(actual)
                    yield ConsumerWindow(actual)

        with monkeypatch.context() as observed:
            observed.setattr(storage, "verified", guarded)
            reader = (
                initialize_upload_identity(
                    storage,
                    candidate,
                    byte_limit=len(body),
                    control_capacity=1024,
                    prepare_payload=lambda: None,
                )
                if initialize
                else open_upload_identity(storage, byte_limit=len(body))
            )
            try:
                with reader as stream:
                    assert reads == []
                    first = stream.read(7)
                    assert first == body[:7]
                    if interrupted:
                        raise ConsumerFailure("consumer interrupted")
                    chunks = [first]
                    while chunk := stream.read(31):
                        chunks.append(chunk)
                    assert b"".join(chunks) == body
            except ConsumerFailure:
                assert interrupted and reads == [7]
            else:
                assert not interrupted
            assert len(retained) == 1 and retained[0].closed and stream.closed
        exact = storage.read_control("upload-identity", byte_limit=1024)
        assert identity_body(storage, byte_limit=len(body)) == body
    with opened(path, backend, initialize=False) as storage:
        assert storage.read_control("upload-identity", byte_limit=1024) == exact
        assert identity_body(storage, byte_limit=len(body)) == body
        with pytest.raises(FileExistsError):
            with initialize_upload_identity(
                storage,
                candidate_body(storage, b"replacement"),
                byte_limit=len(body),
                control_capacity=1024,
                prepare_payload=lambda: None,
            ):
                pytest.fail("retained stream state was replaced")


@pytest.mark.skipif(os.name != "posix", reason="actual POSIX and Unix-VFS identity adapters")
@pytest.mark.parametrize("backend", ["posix", "sqlite"])
@pytest.mark.parametrize("changed", ["length", "hash"])
def test_identity_stream_rejects_a_mismatched_original_before_exposing_a_reader(
    tmp_path, backend, changed
):
    path = tmp_path / ("directory" if backend == "posix" else "identity.sqlite3")
    body = b"original identity"
    with opened(path, backend, initialize=True) as storage:
        assert (
            initialized_body(
                storage,
                body,
                byte_limit=128,
                control_capacity=1024,
                prepare_payload=lambda: None,
            )
            == body
        )
        original = storage.read_control("upload-identity", byte_limit=1024)
        modified = json.loads(original)
        modified["bytes" if changed == "length" else "sha256"] = (
            str(len(body) + 1) if changed == "length" else "0" * 64
        )
        encoded = canonical_json_bytes(modified)
        storage.put_control("upload-identity", encoded, byte_limit=1024, expected=original)
        with pytest.raises(PrimaryStateError):
            with open_upload_identity(storage, byte_limit=128):
                pytest.fail("unverified identity reached the consumer")
        assert storage.read_control("upload-identity", byte_limit=1024) == encoded
        storage.put_control("upload-identity", original, byte_limit=1024, expected=encoded)
        assert identity_body(storage, byte_limit=128) == body


@pytest.mark.skipif(os.name != "posix", reason="actual POSIX and Unix-VFS identity adapters")
@pytest.mark.parametrize("backend", ["posix", "sqlite"])
def test_empty_opaque_identity_stream_keeps_the_exact_zero_length_commitment(tmp_path, backend):
    path = tmp_path / ("directory" if backend == "posix" else "identity.sqlite3")
    with opened(path, backend, initialize=True) as storage:
        with initialize_upload_identity(
            storage,
            candidate_body(storage, b""),
            byte_limit=1,
            control_capacity=1024,
            prepare_payload=lambda: None,
        ) as stream:
            assert stream.read(1) == b""
        assert stream.closed
        reference = json.loads(storage.read_control("upload-identity", byte_limit=1024))
        assert reference["bytes"] == "0" and reference["sha256"] == hashlib.sha256(b"").hexdigest()
    with opened(path, backend, initialize=False) as storage:
        with open_upload_identity(storage, byte_limit=1) as stream:
            assert stream.read(1) == b""
        assert stream.closed


def _record_document(members, *, extra_bytes=0):
    root = Path("/fixture")
    return {
        "format": directory_upload._FORMAT,
        "idempotency_key": 'quote-"',
        "root": str(root),
        "source_naming_view_id": "urn:uuid:11111111-1111-4111-8111-111111111111",
        "sources": [
            {
                "relative_parts": ["nested", f"member-{index}-雪-😀", 'brace-}quote-"'],
                "artifact_id": f"{index:064x}",
                "bytes": 0 if index == 0 else 11,
                "sha256": hashlib.sha256(b"" if index == 0 else b"equal bytes").hexdigest(),
                "opaque-extra": {
                    "nested": ["\\\\", "line\\n", [True, None, "]}"]],
                    "padding": "x" * extra_bytes,
                },
            }
            for index in range(members)
        ],
    }


class _ShortIdentityReader(io.BytesIO):
    def __init__(self, body, short):
        super().__init__(body)
        self.short = short
        self.returned_bytes = 0
        self.request_sizes = []

    def read(self, size=-1):
        assert type(size) is int and 0 < size <= 64 * 1024
        self.request_sizes.append(size)
        body = super().read(min(size, self.short))
        self.returned_bytes += len(body)
        return body


@pytest.mark.parametrize("members", [0, 1, 3, 128])
@pytest.mark.parametrize("short", [1, 7, 65536])
def test_identity_records_preserve_values_and_complete_bytes_across_short_reads(members, short):
    document = _record_document(members)
    body = canonical_json_bytes(document)
    source = _ShortIdentityReader(body, short)
    records = upload_identity_records.UploadIdentityRecords(source)
    assert records.header() == {key: value for key, value in document.items() if key != "sources"}
    with pytest.raises(ValueError, match="has not completed"):
        _ = records.commitment
    assert list(records.records()) == document["sources"]
    assert records.commitment == (len(body), hashlib.sha256(body).hexdigest())
    assert source.returned_bytes == len(body) and not source.closed
    with pytest.raises(ValueError, match="already have a consumer"):
        list(records.records())


@pytest.mark.parametrize(
    "changed",
    [
        "truncated-header",
        "truncated-record",
        "missing-array-end",
        "missing-object-end",
        "trailing-data",
        "trailing-comma",
        "non-object-row",
        "invalid-escape",
        "duplicate-header",
        "unknown-header",
        "missing-header",
        "non-string-header",
    ],
)
def test_identity_records_reject_incomplete_or_ambiguous_documents(changed):
    body = canonical_json_bytes(_record_document(2))
    if changed == "truncated-header":
        body = body[:5]
    elif changed == "truncated-record":
        body = body[: body.rfind(b'"sha256":') + 12]
    elif changed == "missing-array-end":
        body = body[:-2] + b"}"
    elif changed == "missing-object-end":
        body = body[:-1]
    elif changed == "trailing-data":
        body += b"x"
    elif changed == "trailing-comma":
        body = body[:-2] + b",]}"
    elif changed == "non-object-row":
        body = body.replace(b'"sources":[', b'"sources":[null,', 1)
    elif changed == "invalid-escape":
        root = canonical_json_bytes(str(Path("/fixture")))
        body = body.replace(b'"root":' + root, b'"root":"' + bytes((92, 113)) + b'"', 1)
    elif changed == "duplicate-header":
        body = body.replace(b'{"format":', b'{"format":"duplicate","format":', 1)
    elif changed == "unknown-header":
        body = body.replace(b'{"format":', b'{"unknown":"x","format":', 1)
    elif changed == "missing-header":
        body = body.replace(b'"root":' + canonical_json_bytes(str(Path("/fixture"))) + b",", b"", 1)
    elif changed == "non-string-header":
        body = body.replace(
            b'"root":' + canonical_json_bytes(str(Path("/fixture"))), b'"root":1', 1
        )
    records = upload_identity_records.UploadIdentityRecords(_ShortIdentityReader(body, 1))
    with pytest.raises(ValueError):
        list(records.records())
    with pytest.raises(ValueError, match="has not completed"):
        _ = records.commitment


@pytest.mark.parametrize("changed", ["header", "id", "source", "trailing"])
def test_identity_replay_rejects_changed_bytes_before_exposing_an_upload(monkeypatch, changed):
    document = _record_document(3)
    original = canonical_json_bytes(document)
    current = [
        {key: row[key] for key in ("relative_parts", "bytes", "sha256")}
        for row in document["sources"]
    ]
    modified = json.loads(original)
    if changed == "header":
        modified["source_naming_view_id"] = "urn:uuid:changed"
    elif changed == "id":
        modified["sources"][-1]["artifact_id"] = "f" * 64
    elif changed == "source":
        modified["sources"][-1]["bytes"] += 1
    replacement = canonical_json_bytes(modified) + (b"x" if changed == "trailing" else b"")

    class ChangedReader(_ShortIdentityReader):
        def seek(self, offset, whence=0):
            assert offset == 0 and whence == 0
            io.BytesIO.seek(self, 0)
            io.BytesIO.truncate(self, 0)
            io.BytesIO.write(self, replacement)
            return io.BytesIO.seek(self, 0)

    def forbidden(*args, **kwargs):
        pytest.fail("changed original bytes exposed an upload or generated replacement IDs")

    monkeypatch.setattr(directory_upload, "LocalUpload", forbidden)
    monkeypatch.setattr(directory_upload.secrets, "token_hex", forbidden)
    with pytest.raises(ValueError, match="changed during replay|trailing document"):
        directory_upload._load_upload_identity(
            ChangedReader(original, 7),
            Path("/fixture"),
            document["idempotency_key"],
            current,
            open_original=lambda: pytest.fail("invalid preflight reached a replay reader"),
        )


@pytest.mark.parametrize("members", [32, 128, 1024])
@pytest.mark.parametrize("extra_bytes", [0, 512])
def test_identity_record_framing_reads_and_decodes_each_original_value_once(
    monkeypatch, members, extra_bytes
):
    body = canonical_json_bytes(_record_document(members, extra_bytes=extra_bytes))
    source = _ShortIdentityReader(body, 4096)
    decode_sizes = []
    decode = upload_identity_records.json.loads

    def observed(raw):
        decode_sizes.append(len(raw))
        return decode(raw)

    monkeypatch.setattr(upload_identity_records, "json", SimpleNamespace(loads=observed))
    records = upload_identity_records.UploadIdentityRecords(source)
    count = 0
    for row in records.records():
        assert row["artifact_id"] == f"{count:064x}"
        count += 1
    assert count == members
    assert len(decode_sizes) == members + 9
    assert sum(decode_sizes) <= len(body)
    assert source.returned_bytes == len(body)
    assert all(size == 64 * 1024 for size in source.request_sizes)
    assert records.commitment == (len(body), hashlib.sha256(body).hexdigest())


@pytest.mark.parametrize("members", [1, 3, 128, 1024])
def test_fresh_identity_generation_allocates_only_the_consumed_record(monkeypatch, members):
    current = [
        {
            "relative_parts": ["nested", f"member-{index}-雪"],
            "bytes": 0 if index % 3 == 0 else 11,
            "sha256": hashlib.sha256(b"" if index % 3 == 0 else b"equal bytes").hexdigest(),
        }
        for index in range(members)
    ]
    before = canonical_json_bytes(current)
    allocated = serialized = 0
    view = "01234567-89ab-4def-8123-456789abcdef"

    def allocate(size):
        nonlocal allocated
        assert size == 32 and allocated == serialized
        value = f"{allocated:064x}"
        allocated += 1
        return value

    def serialize(value):
        nonlocal serialized
        assert "sources" not in value
        if "artifact_id" in value:
            assert allocated == serialized + 1
            serialized += 1
        return canonical_json_bytes(value)

    monkeypatch.setattr(directory_upload.secrets, "token_hex", allocate)
    monkeypatch.setattr(directory_upload.uuid, "uuid4", lambda: view)
    monkeypatch.setattr(directory_upload, "canonical_json_bytes", serialize)
    document = directory_upload._new_upload_identity(Path("/source"), "streamed", current)
    assert allocated == serialized == 0
    chunks = directory_upload._upload_identity_chunks(document)
    encoded = bytearray(next(chunks))
    assert allocated == serialized == 0
    for chunk in chunks:
        assert 0 <= allocated - serialized <= 1
        encoded.extend(chunk)
    assert allocated == serialized == members
    expected = {
        "format": "a-riverhog-cli-directory-upload/v1",
        "root": str(Path("/source")),
        "idempotency_key": "streamed",
        "source_naming_view_id": "urn:uuid:" + view,
        "sources": [{**item, "artifact_id": f"{index:064x}"} for index, item in enumerate(current)],
    }
    assert bytes(encoded) == canonical_json_bytes(expected)
    assert canonical_json_bytes(current) == before


@pytest.mark.skipif(os.name != "posix", reason="POSIX no-clobber identity selection")
@pytest.mark.parametrize("winner_sources", ["same", "different"])
def test_streamed_identity_publication_preserves_the_concurrent_original(
    tmp_path, monkeypatch, winner_sources
):
    root, current, winner = _replay_original(tmp_path)
    if winner_sources == "different":
        winner["sources"][-1]["sha256"] = "0" * 64
    exact = canonical_json_bytes(winner)
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    destination = directory_upload._state_path(root, "retained")
    attempts = []
    real_allocate = directory_upload.secrets.token_hex

    def allocate(size):
        value = real_allocate(size)
        attempts.append(value)
        return value

    def another_winner(temporary, selected):
        assert Path(selected) == destination
        assert Path(temporary).read_bytes() != exact
        destination.write_bytes(exact)
        raise FileExistsError("concurrent original selected")

    monkeypatch.setattr(directory_upload.secrets, "token_hex", allocate)
    monkeypatch.setattr(directory_upload.os, "link", another_winner)
    if winner_sources == "different":
        with pytest.raises(ValueError, match="differs from the current local sources"):
            directory_upload._prepare_verified_upload(root, "retained", current)
    else:
        upload = directory_upload._prepare_verified_upload(root, "retained", current)
        assert [item.artifact_id for item in read_upload_sources(upload)] == [
            item["artifact_id"] for item in winner["sources"]
        ]
        assert upload.source_naming_view_id == winner["source_naming_view_id"]
    assert len(attempts) == len(current)
    assert not set(attempts).intersection(item["artifact_id"] for item in winner["sources"])
    assert destination.read_bytes() == exact
    assert not list(destination.parent.glob(".upload-*"))

    def forbidden(*args, **kwargs):
        pytest.fail("a selected original generated replacements")

    monkeypatch.setattr(directory_upload.secrets, "token_hex", forbidden)
    if winner_sources == "different":
        with pytest.raises(ValueError, match="differs from the current local sources"):
            directory_upload._prepare_verified_upload(root, "retained", current, initialize=False)
    else:
        assert (
            directory_upload._prepare_verified_upload(root, "retained", current, initialize=False)
            == upload
        )
    assert destination.read_bytes() == exact


@pytest.mark.parametrize("members", [0, 1, 128, 1024])
@pytest.mark.parametrize("consumption", ["complete", "partial", "interrupted"])
def test_scoped_source_replay_retains_only_consumed_descriptors_and_closes(
    monkeypatch, members, consumption
):
    original = _record_document(members)
    body = canonical_json_bytes(original)
    current = [
        {key: item[key] for key in ("relative_parts", "bytes", "sha256")}
        for item in original["sources"]
    ]
    readers, constructed = [], []
    actual_source = upload_sources.LocalSource

    class ObservedSource(actual_source):
        pass

    def source(**values):
        value = ObservedSource(**values)
        constructed.append(weakref.ref(value))
        return value

    def opened():
        reader = _ShortIdentityReader(body, 65536)
        readers.append(reader)
        return reader

    monkeypatch.setattr(upload_sources, "LocalSource", source)
    upload = directory_upload._load_upload_identity(
        io.BytesIO(body),
        Path(original["root"]),
        original["idempotency_key"],
        current,
        open_original=opened,
    )
    assert upload.source_count == members and not constructed and not readers
    for attempt in range(2):
        count = 0
        try:
            with upload.open_sources() as sources:
                assert not readers[-1].closed and readers[-1].returned_bytes >= len(body)
                for value in sources:
                    item = original["sources"][count]
                    assert (value.artifact_id, value.bytes, value.sha256) == (
                        item["artifact_id"],
                        item["bytes"],
                        item["sha256"],
                    )
                    assert value.path == Path(original["root"]).joinpath(*item["relative_parts"])
                    assert sum(reference() is not None for reference in constructed) <= 2
                    count += 1
                    if attempt == 0 and consumption == "interrupted":
                        raise OSError("consumer interrupted")
                    if attempt == 0 and consumption == "partial":
                        break
        except OSError as error:
            assert attempt == 0 and consumption == "interrupted"
            assert str(error) == "consumer interrupted"
        assert count == (min(1, members) if attempt == 0 and consumption != "complete" else members)
        assert len(readers) == attempt + 1 and readers[-1].closed
        assert all(0 < size <= 65536 for size in readers[-1].request_sizes)
        # One raw commitment pass and one record pass, independent of batch count.
        assert readers[-1].returned_bytes <= 2 * len(body)
        with pytest.raises(StopIteration):
            next(sources)


@pytest.mark.parametrize("changed", ["header", "id", "source", "trailing"])
def test_scoped_source_replay_refuses_changed_original_before_any_descriptor(monkeypatch, changed):
    original = _record_document(3)
    body = canonical_json_bytes(original)
    current = [
        {key: item[key] for key in ("relative_parts", "bytes", "sha256")}
        for item in original["sources"]
    ]
    edited = json.loads(body)
    if changed == "header":
        edited["source_naming_view_id"] = "urn:uuid:22222222-2222-4222-8222-222222222222"
    elif changed == "id":
        edited["sources"][-1]["artifact_id"] = "f" * 64
    elif changed == "source":
        edited["sources"][-1]["sha256"] = "0" * 64
    changed_body = canonical_json_bytes(edited) + (b"x" if changed == "trailing" else b"")
    readers = []

    def opened():
        reader = _ShortIdentityReader(changed_body, 7)
        readers.append(reader)
        return reader

    upload = directory_upload._load_upload_identity(
        io.BytesIO(body),
        Path(original["root"]),
        original["idempotency_key"],
        current,
        open_original=opened,
    )

    def forbidden(**values):
        pytest.fail("changed original exposed a descriptor")

    monkeypatch.setattr(upload_sources, "LocalSource", forbidden)
    with pytest.raises(ValueError, match="changed during replay"):
        with upload.open_sources():
            pytest.fail("changed original exposed an inventory")
    assert len(readers) == 1 and readers[0].closed


def test_scoped_source_replay_readers_advance_and_close_independently():
    original = _record_document(3)
    body = canonical_json_bytes(original)
    current = [
        {key: item[key] for key in ("relative_parts", "bytes", "sha256")}
        for item in original["sources"]
    ]
    readers = []

    def opened():
        reader = _ShortIdentityReader(body, 7)
        readers.append(reader)
        return reader

    upload = directory_upload._load_upload_identity(
        io.BytesIO(body),
        Path(original["root"]),
        original["idempotency_key"],
        current,
        open_original=opened,
    )
    with upload.open_sources() as first:
        assert next(first).artifact_id == f"{0:064x}"
        with upload.open_sources() as second:
            assert next(second).artifact_id == f"{0:064x}"
            assert next(first).artifact_id == f"{1:064x}"
            assert next(second).artifact_id == f"{1:064x}"
        assert readers[1].closed and not readers[0].closed
        assert next(first).artifact_id == f"{2:064x}"
        assert next(first, None) is None
    assert all(reader.closed for reader in readers)


@pytest.mark.parametrize("ending", ["additional", "failed"])
def test_identity_preflight_requires_complete_ordered_metadata_exhaustion(tmp_path, ending):
    root = tmp_path / "source"
    root.mkdir()
    (root / "member").write_bytes(b"original")
    root, rows = verified_upload_sources(root, "retained")
    body = canonical_json_bytes(identity_document(root, "retained", rows))

    class IncompleteMetadata:
        def __len__(self):
            return len(rows)

        def __iter__(self):
            yield from rows
            if ending == "additional":
                yield rows[0]
            else:
                raise PrimaryStateError("ordered metadata completeness check failed")

    error = ValueError if ending == "additional" else PrimaryStateError
    with pytest.raises(error, match="current local sources|completeness check failed"):
        directory_upload._load_upload_identity(
            io.BytesIO(body),
            root,
            "retained",
            IncompleteMetadata(),
            open_original=lambda: io.BytesIO(body),
        )


@pytest.mark.skipif(os.name != "posix", reason="actual POSIX and Unix-VFS identity adapters")
@pytest.mark.parametrize("backend", ["posix", "sqlite"])
@pytest.mark.parametrize("size", [0, 4095, 4096, 131073])
def test_streamed_identity_staging_preserves_exact_commitment_and_bounded_copy_windows(
    tmp_path, monkeypatch, backend, size
):
    from a_riverhog_cli.upload_identity import stage_upload_identity

    path = tmp_path / ("directory" if backend == "posix" else "identity.sqlite3")
    body = (b"original\0identity" * (size // 17 + 1))[:size]
    phase = "stage"
    prepared = []
    windows = []
    closed = []

    def source():
        try:
            for offset in range(0, len(body), 137):
                yield body[offset : offset + 137]
        finally:
            closed.append(True)

    with opened(path, backend, initialize=True) as storage:
        put = storage.put_bytes

        def ready():
            assert storage.exists("upload-identity") is (phase == "copy")
            prepared.append(phase)

        def observed(name, chunks, **kwargs):
            def checked():
                for chunk in chunks:
                    assert 0 < len(chunk) <= 4096
                    assert prepared[-1] == phase
                    windows.append((phase, len(chunk)))
                    yield chunk

            return put(name, checked(), **kwargs)

        monkeypatch.setattr(storage, "put_bytes", observed)
        staged = stage_upload_identity(
            storage, source(), byte_limit=max(1, size), prepare_payload=ready
        )
        assert staged.bytes == len(body) and staged.sha256 == hashlib.sha256(body).hexdigest()
        assert closed == [True]
        assert not storage.exists("upload-identity") and not storage.exists("source-identity")
        phase = "copy"
        with initialize_upload_identity(
            storage,
            staged,
            byte_limit=max(1, size),
            control_capacity=1024,
            prepare_payload=ready,
        ) as selected:
            assert selected.read() == body
        assert selected.closed
        assert sum(length for phase, length in windows if phase == "stage") == size
        assert sum(length for phase, length in windows if phase == "copy") == size
        with storage.verified(staged) as retained:
            assert retained.read() == body
        assert storage.exists(staged.name)
    with opened(path, backend, initialize=False) as storage:
        assert identity_body(storage, byte_limit=max(1, size)) == body
        with storage.verified(staged) as retained:
            assert retained.read() == body


@pytest.mark.skipif(os.name != "posix", reason="actual POSIX and Unix-VFS identity adapters")
@pytest.mark.parametrize("backend", ["posix", "sqlite"])
@pytest.mark.parametrize("fault", ["absent", "length", "hash"])
def test_identity_selection_refuses_unverified_staging_before_original_reference(
    tmp_path, backend, fault
):
    from riverhog_client import PrimaryByteFile

    path = tmp_path / ("directory" if backend == "posix" else "identity.sqlite3")
    with opened(path, backend, initialize=True) as storage:
        original = candidate_body(storage, b"original staging")
        changed = PrimaryByteFile(
            "absent-stage" if fault == "absent" else original.name,
            original.bytes + (fault == "length"),
            "0" * 64 if fault == "hash" else original.sha256,
        )

        def forbidden():
            pytest.fail("unverified staging reached final payload preparation")

        with pytest.raises((FileNotFoundError, PrimaryStateError)):
            with initialize_upload_identity(
                storage, changed, byte_limit=128, control_capacity=1024, prepare_payload=forbidden
            ):
                pytest.fail("unverified staging was exposed as original identity")
        assert not storage.exists("upload-identity") and not storage.exists("source-identity")
        with storage.verified(original) as retained:
            assert retained.read() == b"original staging"


@pytest.mark.skipif(os.name != "posix", reason="actual POSIX and Unix-VFS identity adapters")
@pytest.mark.parametrize("backend", ["posix", "sqlite"])
@pytest.mark.parametrize("fault", ["input", "prepare"])
def test_identity_staging_failure_closes_input_without_selecting_original_authority(
    tmp_path, backend, fault
):
    from a_riverhog_cli.upload_identity import stage_upload_identity

    path = tmp_path / ("directory" if backend == "posix" else "identity.sqlite3")

    class Input:
        closed = 0

        def __iter__(self):
            return self

        def __next__(self):
            raise OSError("input unavailable")

        def close(self):
            self.closed += 1

    source = Input()
    with opened(path, backend, initialize=True) as storage:

        def ready():
            if fault == "prepare":
                raise OSError("preparation unavailable")

        with pytest.raises(OSError, match="unavailable"):
            stage_upload_identity(storage, source, byte_limit=8192, prepare_payload=ready)
        assert source.closed == 1
    with opened(path, backend, initialize=False) as storage:
        assert not storage.exists("upload-identity") and not storage.exists("source-identity")
        with pytest.raises(FileNotFoundError):
            identity_body(storage, byte_limit=8192)


@pytest.mark.skipif(os.name != "posix", reason="actual POSIX and Unix-VFS identity adapters")
@pytest.mark.parametrize("backend", ["posix", "sqlite"])
@pytest.mark.parametrize("retained", ["stage", "reference", "content"])
def test_identity_staging_refuses_retained_roles_before_consuming_or_preparing_input(
    tmp_path, backend, retained
):
    from a_riverhog_cli.upload_identity import stage_upload_identity

    path = tmp_path / ("directory" if backend == "posix" else "identity.sqlite3")
    with opened(path, backend, initialize=True) as storage:
        if retained == "reference":
            storage.prepare_controls(("upload-identity",), byte_limit=1024)
            storage.put_control("upload-identity", b"retained original", byte_limit=1024)
        else:
            name = "staged-source-identity" if retained == "stage" else "source-identity"
            storage.put_bytes(name, (b"retained original",), byte_limit=1024)

        def forbidden():
            pytest.fail("retained identity reached input consumption or preparation")

        class Input:
            def __iter__(self):
                forbidden()

        with pytest.raises(FileExistsError, match="conflicts with retained identity"):
            stage_upload_identity(storage, Input(), byte_limit=1024, prepare_payload=forbidden)
