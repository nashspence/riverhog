from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from threading import Event

import pytest
from a_riverhog_cli import directory_upload
from a_riverhog_cli.upload_identity import initialize_upload_identity, read_upload_identity
from riverhog_canonical_json import canonical_json_bytes
from riverhog_client import PrimaryStateCapacity, PrimaryStateError

from tests.support.prepared_primary import SqlitePrimaryTestGuard


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
    root, current = directory_upload._verified_upload_sources(root, "retained")
    original = directory_upload._new_upload_identity(root, "retained", current)
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
        upload = directory_upload._load_upload_identity(loaded, root, key, current)
        assert tuple(source.artifact_id for source in upload.sources) == tuple(
            source["artifact_id"] for source in original["sources"]
        )
        assert upload.source_naming_view_id == original["source_naming_view_id"]
    else:
        with pytest.raises(ValueError):
            directory_upload._load_upload_identity(loaded, root, key, current)
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
            initialize_upload_identity(
                storage, body, byte_limit=len(body), prepare_payload=prepared
            )
            == body
        )
        assert events == ["prepared"]
        exact = storage.read_control("upload-identity", byte_limit=1024)
    with opened(path, backend, initialize=False) as storage:
        assert read_upload_identity(storage, byte_limit=len(body)) == body
        assert storage.read_control("upload-identity", byte_limit=1024) == exact
        with pytest.raises(FileExistsError, match="conflicts with retained identity"):
            initialize_upload_identity(
                storage, b"replacement", byte_limit=len(body), prepare_payload=lambda: None
            )
        assert read_upload_identity(storage, byte_limit=len(body)) == body


@pytest.mark.skipif(os.name != "posix", reason="actual POSIX and Unix-VFS identity adapters")
@pytest.mark.parametrize("backend", ["posix", "sqlite"])
def test_identity_capacity_refusal_does_not_prepare_or_publish_state(tmp_path, backend):
    path = tmp_path / ("directory" if backend == "posix" else "identity.sqlite3")
    with opened(path, backend, initialize=True) as storage:

        def forbidden():
            pytest.fail("unadmitted identity reached preparation")

        with pytest.raises(PrimaryStateCapacity):
            initialize_upload_identity(
                storage, b"original", byte_limit=7, prepare_payload=forbidden
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
            initialize_upload_identity(
                storage, b"original", byte_limit=64, prepare_payload=unavailable
            )
        exact = storage.read_control("upload-identity", byte_limit=1024)
        assert not storage.exists("source-identity")
    with opened(path, backend, initialize=False) as storage:
        with pytest.raises(FileNotFoundError):
            read_upload_identity(storage, byte_limit=64)
        with pytest.raises(FileExistsError):
            initialize_upload_identity(
                storage, b"different", byte_limit=64, prepare_payload=lambda: None
            )
        assert storage.read_control("upload-identity", byte_limit=1024) == exact


@pytest.mark.skipif(os.name != "posix", reason="actual POSIX and Unix-VFS identity adapters")
@pytest.mark.parametrize("backend", ["posix", "sqlite"])
@pytest.mark.parametrize("missing", ["reference", "content", "invalid-reference"])
def test_missing_or_invalid_identity_half_never_reconstructs_authority(tmp_path, backend, missing):
    path = tmp_path / ("directory" if backend == "posix" else "identity.sqlite3")
    with opened(path, backend, initialize=True) as storage:
        initialize_upload_identity(
            storage, b"original", byte_limit=64, prepare_payload=lambda: None
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
            read_upload_identity(storage, byte_limit=64)
        with pytest.raises(FileExistsError):
            initialize_upload_identity(
                storage, b"replacement", byte_limit=64, prepare_payload=lambda: None
            )


@pytest.mark.skipif(os.name != "posix", reason="actual POSIX and Unix-VFS identity adapters")
@pytest.mark.parametrize("backend", ["posix", "sqlite"])
def test_smaller_replay_admission_keeps_original_identity_for_larger_offer(tmp_path, backend):
    path = tmp_path / ("directory" if backend == "posix" else "identity.sqlite3")
    with opened(path, backend, initialize=True) as storage:
        initialize_upload_identity(
            storage, b"original", byte_limit=64, prepare_payload=lambda: None
        )
        exact = storage.read_control("upload-identity", byte_limit=1024)
        with pytest.raises(PrimaryStateCapacity):
            read_upload_identity(storage, byte_limit=7)
        assert storage.read_control("upload-identity", byte_limit=1024) == exact
        assert read_upload_identity(storage, byte_limit=64) == b"original"


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
            return initialize_upload_identity(
                storage, b"replacement", byte_limit=64, prepare_payload=lambda: None
            )

        with ThreadPoolExecutor(max_workers=2) as workers:
            first = workers.submit(
                initialize_upload_identity,
                storage,
                b"original",
                byte_limit=64,
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
        assert read_upload_identity(storage, byte_limit=64) == b"original"
    with opened(path, backend, initialize=False) as storage:
        assert storage.read_control("upload-identity", byte_limit=1024) == exact
        assert read_upload_identity(storage, byte_limit=64) == b"original"


@pytest.mark.skipif(os.name != "nt", reason="qualified Windows engine and private local NTFS")
def test_native_windows_upload_identity_keeps_exact_ids_and_never_reinitializes_missing_state(
    tmp_path,
):
    code = r"""
import importlib.util,json,sys
from pathlib import Path
root=Path(sys.argv[1]);sys.path.insert(0,str(root))
from a_riverhog_cli import directory_upload
from a_riverhog_cli.upload_identity import initialize_upload_identity,read_upload_identity
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
source,current=directory_upload._verified_upload_sources(source,"retained")
document=directory_upload._new_upload_identity(source,"retained",current)
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
        assert initialize_upload_identity(
            identity,body,byte_limit=8192,prepare_payload=prepare)==body
    def forbidden(*args,**kwargs):
        raise AssertionError("resume generated replacement identities")
    directory_upload.secrets.token_hex=forbidden
    with factory(path,allocation_descriptor=descriptor,initialize=False) as storage:
        identity=storage.child("directory-upload",initialize=False)
        assert read_upload_identity(identity,byte_limit=8192)==body
        loaded=directory_upload._load_upload_identity(
            json.loads(body),source,"retained",current)
        assert loaded.sources[0].artifact_id==document["sources"][0]["artifact_id"]
        assert loaded.source_naming_view_id==document["source_naming_view_id"]
        exact=identity.read_control("upload-identity",byte_limit=1024)
        identity.remove_control("upload-identity",expected=exact)
    with factory(path,allocation_descriptor=descriptor,initialize=False) as storage:
        identity=storage.child("directory-upload",initialize=False)
        try:read_upload_identity(identity,byte_limit=8192)
        except FileNotFoundError:pass
        else:raise AssertionError("missing original reference was reconstructed")
        try:initialize_upload_identity(
            identity,b"replacement",byte_limit=8192,prepare_payload=lambda:None)
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
