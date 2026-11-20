from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import weakref
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from a_riverhog_cli import directory_upload, upload_identity, upload_windows
from a_riverhog_cli.upload_identity import open_upload_identity
from a_riverhog_cli.upload_sqlite import UploadSqlitePolicy, UploadSqliteResources
from a_riverhog_cli.upload_windows import WindowsUploadState
from riverhog_client import (
    PrimaryStateBusy,
    PrimaryStateCapacity,
    PrimaryStatePolicy,
    SqliteOperationBank,
    SqliteOperationGrants,
    SqlitePrimaryLimits,
    SqliteReadinessPlan,
    primary_allocation_descriptor,
)

from tests.support.cli_upload import read_upload_sources
from tests.unit.test_cli_upload_sqlite_process import _Process


def identity_body(storage, *, byte_limit):
    with open_upload_identity(storage, byte_limit=byte_limit) as stream:
        return stream.read()


def selection(parent, process=None, resources=None):
    bank = SqliteOperationBank(
        1024, 8 * 1024**2, 65536, 8 * 1024**2, 65536, 262144, 0, 1 << 40, 1 << 60, 1 << 40
    )
    return WindowsUploadState(
        parent=parent,
        process=process,
        resources=resources,
        limits=SqlitePrimaryLimits(16 * 1024**2, 32 * 1024**2, 128 * 1024),
        operations=SqliteOperationGrants(bank, bank),
        retry_profile=(2, 3),
        readiness=SqliteReadinessPlan((1033, 16), 65536, 8, 128, 1024 * 1024),
        identity_bytes=8192,
        preparation_steps=512,
    )


@pytest.mark.parametrize("ready_after", [1, 3, None, "error"])
def test_producer_preparation_spends_only_its_explicit_execution_grant(
    tmp_path, monkeypatch, ready_after
):
    calls = []
    storage = object()
    chosen = replace(selection(tmp_path), preparation_steps=3)

    def step(self, supplied):
        assert self is chosen and supplied is storage
        calls.append(supplied)
        if ready_after == "error":
            raise OSError("fixture preparation failure")
        return len(calls) == ready_after

    monkeypatch.setattr(WindowsUploadState, "preparation_step", step)
    if ready_after == "error":
        with pytest.raises(OSError, match="fixture preparation failure"):
            chosen.prepare_storage(storage)
        assert len(calls) == 1
    else:
        assert chosen.prepare_storage(storage) is (ready_after is not None)
        assert len(calls) == (ready_after or chosen.preparation_steps)


@pytest.mark.parametrize("field", ["identity_bytes", "preparation_steps"])
@pytest.mark.parametrize("value", [True, 0, -1, 1.5])
def test_windows_upload_admissions_are_explicit_positive_integers(tmp_path, field, value):
    with pytest.raises(ValueError, match="admissions must be positive"):
        replace(selection(tmp_path), **{field: value})


@pytest.mark.parametrize("parent", [Path("relative"), "/absolute/string"])
def test_windows_upload_requires_an_explicit_path_without_creating_parents(parent):
    with pytest.raises(ValueError, match="absolute durable parent"):
        selection(parent)


class _SqlOnlyProcess(_Process):
    """Exercise the app binding with real Unix SQL; not a Windows engine witness."""

    def __init__(self):
        super().__init__()
        self.opens = []
        self.storage = None

    def primary_location(self, parent, allocation_descriptor):
        self.verify()
        return parent / (hashlib.sha256(allocation_descriptor).hexdigest() + ".sqlite3")

    def primary_factory(self, *, limits, operations, retry_profile, connection_factory):
        from riverhog_client._prepared_primary_sqlite import TransactionalPrimaryFiles

        from tests.support.prepared_primary import SqlitePrimaryTestGuard

        self.verify()

        def opened(location, *, allocation_descriptor, initialize):
            self.verify()
            if initialize:
                location.touch(exist_ok=False)
            elif not location.is_file():
                raise FileNotFoundError(location.name)
            self.opens.append(initialize)
            self.storage = TransactionalPrimaryFiles(
                SqlitePrimaryTestGuard(location),
                allocation_descriptor=allocation_descriptor,
                limits=limits,
                operations=operations,
                initialize=initialize,
                connection_factory=connection_factory,
            )
            return self.storage

        return opened


@pytest.fixture
def binding(tmp_path, monkeypatch):
    if os.name != "posix":
        pytest.skip("actual Unix-VFS application binding fixture")
    source = tmp_path / "source"
    source.mkdir()
    (source / "member").write_bytes(b"original source")
    parent = tmp_path / "retained"
    parent.mkdir()
    state = tmp_path / "unused-posix-state"
    monkeypatch.setenv("XDG_STATE_HOME", str(state))

    def forbidden(*args, **kwargs):
        pytest.fail("Windows selection reached POSIX state placement")

    monkeypatch.setattr(directory_upload, "_state_path", forbidden)
    process = _SqlOnlyProcess()
    resources = UploadSqliteResources(UploadSqlitePolicy(connections=1, blobs=1), process=process)
    chosen = selection(parent, process, resources)
    policy = PrimaryStatePolicy(
        scope={"service": "fixture-service", "account": "fixture-account"},
        control_bytes=1024,
        inventory_order_bytes=65536,
    )
    descriptor = primary_allocation_descriptor(
        allocation_id=directory_upload._allocation_id(source, "retained"),
        scope=policy.scope,
        limits=policy.limits(),
    )
    path = process.primary_location(parent, descriptor)
    try:
        yield source, state, chosen, policy, descriptor, path
    finally:
        resources.close()
    assert not state.exists()


def open_upload(binding, *, initialize, chosen=None):
    source, _state, selected, policy, _descriptor, _path = binding
    return directory_upload.prepare_primary_upload(
        source,
        "retained",
        policy,
        initialize=initialize,
        windows=selected if chosen is None else chosen,
    )


def forbid_reconstruction(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("retained state admission reached source reconstruction or new IDs")

    monkeypatch.setattr(directory_upload, "_verified_upload_sources", forbidden)
    monkeypatch.setattr(directory_upload, "_verified_ordered_upload_sources", forbidden)
    monkeypatch.setattr(directory_upload.secrets, "token_hex", forbidden)


@pytest.mark.skipif(os.name != "posix", reason="actual Unix-VFS application binding fixture")
def test_windows_selection_binds_actual_primary_and_reopens_original_source_allocation(
    binding, monkeypatch
):
    source, _state, chosen, _policy, _descriptor, path = binding
    with open_upload(binding, initialize=True) as (first, store):
        assert read_upload_sources(first)[0].path == source / "member"
        assert store.allocation.allocation_id == directory_upload._allocation_id(source, "retained")
        assert not store.publication_started
        identity = chosen.process.storage.child("directory-upload", initialize=False)
        exact = identity_body(identity, byte_limit=chosen.identity_bytes)
    assert path.is_file() and chosen.resources.status.connections == 0

    def no_new_ids(*args, **kwargs):
        pytest.fail("resume generated replacement source IDs")

    monkeypatch.setattr(directory_upload.secrets, "token_hex", no_new_ids)
    with open_upload(binding, initialize=False) as (resumed, store):
        assert resumed == first and not store.publication_started
        identity = chosen.process.storage.child("directory-upload", initialize=False)
        assert identity_body(identity, byte_limit=chosen.identity_bytes) == exact
    assert chosen.process.opens == [True, False]
    assert chosen.resources.status.connection_peak == 1
    assert chosen.resources.status.connections == chosen.resources.status.blobs == 0
    assert chosen.resources.pending_directories == ()


@pytest.mark.skipif(os.name != "posix", reason="actual Unix-VFS application binding fixture")
def test_fresh_windows_selection_refuses_retained_container_before_enumeration(
    binding, monkeypatch
):
    with open_upload(binding, initialize=True) as (original, _store):
        pass
    with monkeypatch.context() as blocked:
        forbid_reconstruction(blocked)

        def no_native_open(*args, **kwargs):
            pytest.fail("known retained primary conflict reached native construction")

        blocked.setattr(binding[2].process, "primary_factory", no_native_open)
        with pytest.raises(FileExistsError, match="conflicts with retained primary state"):
            with open_upload(binding, initialize=True):
                pytest.fail("fresh authority replaced retained source state")
    with open_upload(binding, initialize=False) as (resumed, _store):
        assert resumed == original


@pytest.mark.skipif(os.name != "posix", reason="actual Unix-VFS application binding fixture")
@pytest.mark.parametrize("missing", ["container", "namespace", "reference", "payload"])
def test_windows_resume_reads_original_identity_before_any_source_reconstruction(
    binding, monkeypatch, missing
):
    _source, _state, chosen, _policy, descriptor, path = binding
    if missing != "namespace":
        with open_upload(binding, initialize=True):
            pass
    else:
        # A valid allocated primary without the application namespace cannot
        # authorize source initialization on resume.
        from riverhog_client import PrimaryStore

        with PrimaryStore(
            path,
            allocation_id=directory_upload._allocation_id(binding[0], "retained"),
            scope=binding[3].scope,
            limits=binding[3].limits(),
            initialize=True,
            storage_factory=chosen.process.primary_factory(
                limits=chosen.limits,
                operations=chosen.operations,
                retry_profile=chosen.retry_profile,
                connection_factory=chosen.resources.connect_primary,
            ),
        ):
            pass
    if missing == "container":
        path.unlink()
    elif missing in ("reference", "payload"):
        factory = chosen.process.primary_factory(
            limits=chosen.limits,
            operations=chosen.operations,
            retry_profile=chosen.retry_profile,
            connection_factory=chosen.resources.connect_primary,
        )
        with factory(path, allocation_descriptor=descriptor, initialize=False) as physical:
            identity = physical.child("directory-upload", initialize=False)
            if missing == "reference":
                exact = identity.read_control("upload-identity", byte_limit=1024)
                identity.remove_control("upload-identity", expected=exact)
            else:
                for _ in range(16):
                    if identity.remove_step("source-identity"):
                        break
                else:
                    pytest.fail("fixture payload cleanup did not finish")
    forbid_reconstruction(monkeypatch)
    with pytest.raises(FileNotFoundError):
        with open_upload(binding, initialize=False):
            pytest.fail("incomplete retained state entered publication")
    assert path.is_file() is (missing != "container")


@pytest.mark.skipif(os.name != "posix", reason="actual Unix-VFS application binding fixture")
def test_changed_source_refusal_preserves_original_windows_identity(binding, monkeypatch):
    source, _state, chosen, _policy, _descriptor, _path = binding
    with open_upload(binding, initialize=True) as (original, _store):
        identity = chosen.process.storage.child("directory-upload", initialize=False)
        exact = identity_body(identity, byte_limit=chosen.identity_bytes)
    (source / "member").write_bytes(b"changed source")

    def no_new_ids(*args, **kwargs):
        pytest.fail("changed source allocated replacement IDs")

    monkeypatch.setattr(directory_upload.secrets, "token_hex", no_new_ids)
    with pytest.raises(ValueError, match="differs from the current local sources"):
        with open_upload(binding, initialize=False):
            pytest.fail("changed source entered publication")
    (source / "member").write_bytes(b"original source")
    with open_upload(binding, initialize=False) as (resumed, _store):
        assert resumed == original
        identity = chosen.process.storage.child("directory-upload", initialize=False)
        assert identity_body(identity, byte_limit=chosen.identity_bytes) == exact


@pytest.mark.skipif(os.name != "posix", reason="actual Unix-VFS application binding fixture")
def test_identity_byte_refusal_retains_allocation_and_never_invents_resume_identity(
    binding, monkeypatch
):
    chosen = replace(binding[2], identity_bytes=1)
    with pytest.raises(PrimaryStateCapacity):
        with open_upload(binding, initialize=True, chosen=chosen):
            pytest.fail("unadmitted source identity entered publication")
    assert binding[5].is_file()
    forbid_reconstruction(monkeypatch)
    with pytest.raises(FileNotFoundError):
        with open_upload(binding, initialize=False):
            pytest.fail("missing source identity was initialized")


@pytest.mark.skipif(os.name != "posix", reason="actual Unix-VFS application binding fixture")
def test_preparation_quantum_exhaustion_retains_original_reference_and_stops_resume(
    binding, monkeypatch
):
    chosen = binding[2]
    actual_step = WindowsUploadState.preparation_step

    def withhold_after_reference(self, storage):
        identity = storage.child("directory-upload", initialize=False)
        if identity.exists("upload-identity"):
            return False
        return actual_step(self, storage)

    monkeypatch.setattr(WindowsUploadState, "preparation_step", withhold_after_reference)
    with pytest.raises(PrimaryStateBusy, match="another execution grant"):
        with open_upload(binding, initialize=True, chosen=chosen):
            pytest.fail("incomplete preparation entered publication")
    factory = chosen.process.primary_factory(
        limits=chosen.limits,
        operations=chosen.operations,
        retry_profile=chosen.retry_profile,
        connection_factory=chosen.resources.connect_primary,
    )
    with factory(binding[5], allocation_descriptor=binding[4], initialize=False) as physical:
        identity = physical.child("directory-upload", initialize=False)
        exact = identity.read_control("upload-identity", byte_limit=1024)
        assert json.loads(exact)["bytes"] != "0"
        assert not identity.exists("source-identity")
    forbid_reconstruction(monkeypatch)
    with pytest.raises(FileNotFoundError):
        with open_upload(binding, initialize=False):
            pytest.fail("interrupted preparation allocated replacement source identity")


@pytest.mark.skipif(os.name != "posix", reason="actual Unix-VFS application binding fixture")
def test_lost_process_refuses_windows_placement_before_source_or_state(binding, monkeypatch):
    binding[2].process.accepted = False
    forbid_reconstruction(monkeypatch)
    with pytest.raises(RuntimeError, match="no longer admitted"):
        with open_upload(binding, initialize=True):
            pytest.fail("lost process entered upload state")
    assert not binding[5].exists()


@pytest.mark.skipif(os.name != "nt", reason="qualified Windows engine and private local NTFS")
def test_native_windows_application_upload_opens_and_replays_original_allocation(tmp_path):
    code = r"""
def read_upload_sources(upload):
    with upload.open_sources() as sources:
        return tuple(sources)

import importlib.util,json,sys
from pathlib import Path
root=Path(sys.argv[1]);sys.path.insert(0,str(root))
from a_riverhog_cli import directory_upload
from a_riverhog_cli.upload_identity import open_upload_identity
def identity_body(storage, *, byte_limit):
    with open_upload_identity(storage, byte_limit=byte_limit) as stream:
        return stream.read()




from a_riverhog_cli.upload_sqlite import UploadSqlitePolicy,UploadSqliteResources
from a_riverhog_cli.upload_windows import WindowsUploadState
from riverhog_client import (WindowsSqliteProcess,PrimaryStatePolicy,SqlitePrimaryLimits,
    SqliteOperationBank,SqliteOperationGrants,SqliteReadinessPlan,primary_allocation_descriptor)
spec=importlib.util.spec_from_file_location("binding_parent",
    root/"tests/platform/windows_primary_process.py")
parents=importlib.util.module_from_spec(spec);sys.modules[spec.name]=parents
spec.loader.exec_module(parents)
parent=Path(sys.argv[2])/"private-parent";parents._parent(parent)
source=Path(sys.argv[2])/"source";source.mkdir();(source/"member").write_bytes(b"original")
process=WindowsSqliteProcess(heap_bytes=64*1024**2,native_limits=(1,2))
resources=UploadSqliteResources(UploadSqlitePolicy(connections=1,blobs=1),process=process)
bank=SqliteOperationBank(1024,8*1024**2,65536,8*1024**2,65536,
    262144,0,1<<40,1<<60,1<<40)
chosen=WindowsUploadState(parent=parent,process=process,resources=resources,
    limits=SqlitePrimaryLimits(16*1024**2,32*1024**2,128*1024),
    operations=SqliteOperationGrants(bank,bank),retry_profile=(2,3),
    readiness=SqliteReadinessPlan((1033,16),65536,8,128,1024*1024),
    identity_bytes=8192,preparation_steps=512)
policy=PrimaryStatePolicy(scope={"service":"fixture-service","account":"fixture-account"},
    control_bytes=1024,inventory_order_bytes=65536)
def open_upload(initialize):
    return directory_upload.prepare_primary_upload(
        source,"retained",policy,initialize=initialize,windows=chosen)
def forbidden(*args,**kwargs):
    raise AssertionError("resume or conflict generated replacement source authority")
def cached_inventory(upload, store):
    assert store.recover_step()
    changes=store._files._connection.total_changes
    for _ in range(2):
        with upload.open_inventory() as members:
            assert [str(row.artifact_id) for row in members]==[original_id]
        assert list(members)==[]
    assert store._files._connection.total_changes==changes
directory_upload._state_path=forbidden
try:
    with open_upload(True) as (original,store):
        assert not store.publication_started
        original_id=read_upload_sources(original)[0].artifact_id
        cached_inventory(original,store)
    directory_upload.secrets.token_hex=forbidden
    with open_upload(False) as (resumed,store):
        assert resumed==original and not store.publication_started
        cached_inventory(resumed,store)
    saved=directory_upload._verified_upload_sources
    directory_upload._verified_upload_sources=forbidden
    original_ordered=directory_upload._verified_ordered_upload_sources
    directory_upload._verified_ordered_upload_sources=forbidden
    try:
        with open_upload(True):raise AssertionError("fresh initialization replaced state")
    except FileExistsError:pass
    descriptor=primary_allocation_descriptor(
        allocation_id=directory_upload._allocation_id(source,"retained"),
        scope=policy.scope,limits=policy.limits())
    path=process.primary_location(parent,descriptor)
    factory=process.primary_factory(limits=chosen.limits,operations=chosen.operations,
        retry_profile=chosen.retry_profile,connection_factory=resources.connect_primary)
    with factory(path,allocation_descriptor=descriptor,initialize=False) as physical:
        identity=physical.child("directory-upload",initialize=False)
        exact=identity.read_control("upload-identity",byte_limit=1024)
        body=identity_body(identity,byte_limit=8192)
        assert json.loads(body)["sources"][0]["artifact_id"]==original_id
        identity.remove_control("upload-identity",expected=exact)
    try:
        with open_upload(False):raise AssertionError("lost identity reached source reconstruction")
    except FileNotFoundError:pass
    directory_upload._verified_ordered_upload_sources=original_ordered
    from types import SimpleNamespace
    from riverhog_client import PrimaryStateError
    (source/"second").write_bytes(b"equal")
    directory_upload.secrets=SimpleNamespace(token_hex=lambda _size:"a"*64)
    def duplicate(initialize):
        return directory_upload.prepare_primary_upload(
            source,"duplicate",policy,initialize=initialize,windows=chosen)
    try:
        with duplicate(True):raise AssertionError("duplicate original IDs exposed an upload")
    except PrimaryStateError as error:assert "duplicate" in str(error)
    directory_upload.secrets=SimpleNamespace(token_hex=forbidden)
    try:
        with duplicate(False):raise AssertionError("cold duplicate IDs exposed an upload")
    except PrimaryStateError as error:assert "duplicate" in str(error)
    assert resources.status.connections==resources.status.blobs==0
    assert path.is_file() and resources.pending_directories==()
    assert process.native_report()["owners"]==process.native_report()["files"]==0
finally:
    resources.close()
assert process.report()["heap_limit_bytes"]==64*1024**2
print(json.dumps({"passed":True,"original_allocation_replayed":True,"missing_identity_stops":True}))
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", code, str(Path(__file__).resolve().parents[2]), str(tmp_path)],
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout)["passed"]


@pytest.mark.skipif(os.name != "posix", reason="actual Unix-VFS application binding fixture")
def test_selected_application_preparation_reaches_session_barrier_and_reopens_originals(binding):
    chosen = binding[2]
    request = b'{"request":"selected application preparation"}'
    with open_upload(binding, initialize=True) as (original, store):
        assert not store.publication_started
        for _ in range(chosen.preparation_steps):
            try:
                retained = store.session_for(request, storage_preparation=chosen.preparation_step)
            except PrimaryStateBusy:
                continue
            assert retained is None
            break
        else:
            pytest.fail("application session preparation did not finish within its grant")
        assert store.publication_started
    with open_upload(binding, initialize=False) as (resumed, store):
        assert resumed == original and store.publication_started
    assert chosen.resources.status.connections == chosen.resources.status.blobs == 0


@pytest.mark.skipif(os.name != "nt", reason="qualified Windows engine and private local NTFS")
def test_native_windows_selected_preparation_reaches_session_barrier_and_reopens_originals(
    tmp_path,
):
    code = r"""
import importlib.util,json,sys
from pathlib import Path
root=Path(sys.argv[1]);sys.path.insert(0,str(root))
from a_riverhog_cli import directory_upload
from a_riverhog_cli.upload_sqlite import UploadSqlitePolicy,UploadSqliteResources
from a_riverhog_cli.upload_windows import WindowsUploadState
from riverhog_client import (WindowsSqliteProcess,PrimaryStatePolicy,PrimaryStateBusy,
    SqlitePrimaryLimits,SqliteOperationBank,SqliteOperationGrants,SqliteReadinessPlan)
spec=importlib.util.spec_from_file_location("callback_parent",
    root/"tests/platform/windows_primary_process.py")
parents=importlib.util.module_from_spec(spec);sys.modules[spec.name]=parents
spec.loader.exec_module(parents)
parent=Path(sys.argv[2])/"private-parent";parents._parent(parent)
source=Path(sys.argv[2])/"source";source.mkdir();(source/"member").write_bytes(b"original")
process=WindowsSqliteProcess(heap_bytes=64*1024**2,native_limits=(1,2))
resources=UploadSqliteResources(UploadSqlitePolicy(connections=1,blobs=1),process=process)
bank=SqliteOperationBank(1024,8*1024**2,65536,8*1024**2,65536,
    262144,0,1<<40,1<<60,1<<40)
chosen=WindowsUploadState(parent=parent,process=process,resources=resources,
    limits=SqlitePrimaryLimits(16*1024**2,32*1024**2,128*1024),
    operations=SqliteOperationGrants(bank,bank),retry_profile=(2,3),
    readiness=SqliteReadinessPlan((1033,16),65536,8,128,1024*1024),
    identity_bytes=8192,preparation_steps=512)
policy=PrimaryStatePolicy(scope={"service":"fixture-service","account":"fixture-account"},
    control_bytes=1024,inventory_order_bytes=65536)
def opened(initialize):
    return directory_upload.prepare_primary_upload(
        source,"retained",policy,initialize=initialize,windows=chosen)
request=b'{"request":"selected application preparation"}'
try:
    with opened(True) as (original,store):
        assert not store.publication_started
        for _ in range(chosen.preparation_steps):
            try:retained=store.session_for(request,storage_preparation=chosen.preparation_step)
            except PrimaryStateBusy:continue
            assert retained is None
            break
        else:raise AssertionError("application session preparation exceeded its execution grant")
        assert store.publication_started
    def forbidden(*args,**kwargs):
        raise AssertionError("cold session barrier generated replacement source IDs")
    directory_upload.secrets.token_hex=forbidden
    with opened(False) as (resumed,store):
        assert resumed==original and store.publication_started
    assert resources.status.connections==resources.status.blobs==0
    assert process.native_report()["owners"]==process.native_report()["files"]==0
finally:
    resources.close()
assert process.report()["heap_limit_bytes"]==64*1024**2
print(json.dumps({"passed":True,"local_session_barrier":True,"cold_originals_preserved":True}))
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", code, str(Path(__file__).resolve().parents[2]), str(tmp_path)],
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout)["passed"]


@pytest.mark.skipif(os.name != "posix", reason="actual Unix-VFS application binding fixture")
@pytest.mark.parametrize("initialize", [True, False])
def test_windows_binding_releases_source_inventory_before_publication(
    binding, monkeypatch, initialize
):
    if not initialize:
        with open_upload(binding, initialize=True):
            pass
    verified = directory_upload._verified_ordered_upload_sources
    retained = []

    @contextmanager
    def observed(*args, **kwargs):
        with verified(*args, **kwargs) as (selected, current):
            retained.append(weakref.ref(current))
            yield selected, current

    monkeypatch.setattr(directory_upload, "_verified_ordered_upload_sources", observed)
    chosen = binding[2]
    with open_upload(binding, initialize=initialize) as (upload, store):
        assert len(retained) == 1 and retained[0]() is None
        assert not store.publication_started
        assert (
            read_upload_sources(upload)[0].producer_file().source.read_bytes() == b"original source"
        )
        identity = chosen.process.storage.child("directory-upload", initialize=False)
        exact = identity_body(identity, byte_limit=chosen.identity_bytes)
        assert (
            json.loads(exact)["sources"][0]["artifact_id"]
            == read_upload_sources(upload)[0].artifact_id
        )
    assert chosen.resources.status.connections == chosen.resources.status.blobs == 0
    with open_upload(binding, initialize=False) as (replayed, store):
        assert replayed == upload and not store.publication_started
        assert retained[-1]() is None
        identity = chosen.process.storage.child("directory-upload", initialize=False)
        assert identity_body(identity, byte_limit=chosen.identity_bytes) == exact
    assert chosen.resources.status.connections == chosen.resources.status.blobs == 0


@pytest.mark.skipif(os.name != "posix", reason="actual Unix-VFS application binding fixture")
@pytest.mark.parametrize("initialize", [True, False])
def test_windows_identity_reader_is_scoped_through_preflight_and_closed_before_publication(
    binding, monkeypatch, initialize
):
    if not initialize:
        with open_upload(binding, initialize=True):
            pass
    actual_open = upload_identity.open_upload_identity
    verified = directory_upload._verified_ordered_upload_sources
    readers = []
    enumerations = []

    @contextmanager
    def observed(storage, *, byte_limit):
        with actual_open(storage, byte_limit=byte_limit) as stream:
            readers.append(stream)
            yield stream

    @contextmanager
    def enumerate_sources(*args, **kwargs):
        if initialize:
            assert readers == []
        else:
            assert len(readers) == 1 and not readers[0].closed
        enumerations.append(True)
        with verified(*args, **kwargs) as selected:
            yield selected

    monkeypatch.setattr(upload_identity, "open_upload_identity", observed)
    monkeypatch.setattr(upload_windows, "open_upload_identity", observed)
    monkeypatch.setattr(directory_upload, "_verified_ordered_upload_sources", enumerate_sources)
    with open_upload(binding, initialize=initialize) as (upload, store):
        assert enumerations == [True]
        assert len(readers) == 1 and readers[0].closed
        assert (
            read_upload_sources(upload)[0].producer_file().source.read_bytes() == b"original source"
        )
        assert not store.publication_started
    assert readers[0].closed
    assert binding[2].resources.status.connections == binding[2].resources.status.blobs == 0


@pytest.mark.skipif(os.name != "posix", reason="actual Unix-VFS application binding fixture")
def test_source_preparation_exhaustion_stops_before_source_ids_or_original_reference(
    binding, monkeypatch
):
    chosen = replace(binding[2], preparation_steps=1)

    def forbidden(*args, **kwargs):
        pytest.fail("source preparation exhaustion reached source hashing or identity allocation")

    monkeypatch.setattr(directory_upload, "_source_fixity", forbidden)
    monkeypatch.setattr(directory_upload, "uuid", SimpleNamespace(uuid4=forbidden))
    monkeypatch.setattr(directory_upload, "secrets", SimpleNamespace(token_hex=forbidden))
    with pytest.raises(PrimaryStateBusy, match="another execution grant"):
        with open_upload(binding, initialize=True, chosen=chosen):
            pytest.fail("unprepared source ordering entered publication")
    assert chosen.resources.status.connections == chosen.resources.status.blobs == 0
    factory = chosen.process.primary_factory(
        limits=chosen.limits,
        operations=chosen.operations,
        retry_profile=chosen.retry_profile,
        connection_factory=chosen.resources.connect_primary,
    )
    with factory(binding[5], allocation_descriptor=binding[4], initialize=False) as physical:
        identity = physical.child("directory-upload", initialize=False)
        assert not identity.exists("upload-identity")
        assert not identity.exists("source-identity")
    forbid_reconstruction(monkeypatch)
    with pytest.raises(FileNotFoundError):
        with open_upload(binding, initialize=False):
            pytest.fail("missing original identity was reconstructed")
    assert chosen.resources.status.connections == chosen.resources.status.blobs == 0


@pytest.mark.skipif(os.name != "posix", reason="actual Unix-VFS application binding fixture")
@pytest.mark.parametrize("member_count", [1, 3, 128])
def test_windows_source_ordering_verifies_every_member_before_allocating_ids(
    binding, monkeypatch, member_count
):
    source = binding[0]
    for number in range(member_count - 1):
        (source / f"é-{number:04d}").write_bytes(b"" if number % 2 else b"equal")
    expected = sorted(source.iterdir(), key=lambda path: path.relative_to(source).as_posix())
    chosen = replace(binding[2], identity_bytes=256 * 1024)
    actual_fixity = directory_upload._source_fixity
    actual_uuid = directory_upload.uuid.uuid4
    actual_token = directory_upload.secrets.token_hex
    hashed = []

    def forbidden(*args, **kwargs):
        pytest.fail("Windows source ordering retained the complete path or metadata inventory")

    def fixity(path):
        hashed.append(path)
        return actual_fixity(path)

    def fresh_view():
        assert len(hashed) == member_count
        return actual_uuid()

    def fresh_id(size):
        assert len(hashed) == member_count
        return actual_token(size)

    monkeypatch.setattr(directory_upload, "_source_paths", forbidden)
    monkeypatch.setattr(directory_upload, "preview_upload", forbidden)
    monkeypatch.setattr(directory_upload, "_verified_upload_sources", forbidden)
    monkeypatch.setattr(directory_upload, "_source_fixity", fixity)
    monkeypatch.setattr(directory_upload, "uuid", SimpleNamespace(uuid4=fresh_view))
    monkeypatch.setattr(directory_upload, "secrets", SimpleNamespace(token_hex=fresh_id))
    with open_upload(binding, initialize=True, chosen=chosen) as (original, store):
        assert original.source_count == member_count
        sources = read_upload_sources(original)
        assert [item.path for item in sources] == expected
        assert len({item.artifact_id for item in sources}) == member_count
        assert not store.publication_started
        assert chosen.resources.status.connections == 1
        original_ids = tuple(item.artifact_id for item in sources)
    assert chosen.resources.status.connections == chosen.resources.status.blobs == 0
    hashed.clear()
    monkeypatch.setattr(directory_upload, "uuid", SimpleNamespace(uuid4=forbidden))
    monkeypatch.setattr(directory_upload, "secrets", SimpleNamespace(token_hex=forbidden))
    with open_upload(binding, initialize=False, chosen=chosen) as (resumed, store):
        assert resumed == original
        assert tuple(item.artifact_id for item in read_upload_sources(resumed)) == original_ids
        assert len(hashed) == member_count
        assert not store.publication_started
    assert chosen.resources.status.connections == chosen.resources.status.blobs == 0
    assert chosen.resources.status.connection_peak == 1
    assert chosen.resources.pending_directories == ()


@pytest.mark.skipif(os.name != "posix", reason="actual Unix-VFS application binding fixture")
@pytest.mark.parametrize("initialize", [True, False])
def test_windows_canonical_inventory_reuses_original_generation_after_metadata_and_recovery(
    binding, monkeypatch, initialize
):
    if not initialize:
        with open_upload(binding, initialize=True):
            pass
    metadata_live = False
    canonical_calls = 0
    selected = directory_upload._verified_ordered_upload_sources
    prepare = upload_windows.prepare_primary_inventory

    @contextmanager
    def metadata(*args, **kwargs):
        nonlocal metadata_live
        with selected(*args, **kwargs) as values:
            metadata_live = True
            try:
                yield values
            finally:
                metadata_live = False

    def canonical(store, members):
        nonlocal canonical_calls
        assert not metadata_live and not store.publication_started
        canonical_calls += 1
        return prepare(store, members)

    monkeypatch.setattr(directory_upload, "_verified_ordered_upload_sources", metadata)
    monkeypatch.setattr(upload_windows, "prepare_primary_inventory", canonical)
    with open_upload(binding, initialize=initialize) as (upload, store):
        assert canonical_calls == 1
        files = binding[2].process.storage
        original = identity_body(files.child("directory-upload", initialize=False), byte_limit=8192)
        expected = json.loads(original)["sources"][0]["artifact_id"]
        assert files._one("SELECT namespace,phase FROM primary_ordering") == (
            files._namespace,
            "ready",
        )
        assert store.recover_step()
        changes = files._connection.total_changes
        for _ in range(2):
            with upload.open_inventory() as members:
                assert [(str(member.artifact_id), member.bytes) for member in members] == [
                    (expected, len(b"original source"))
                ]
            assert list(members) == []
        assert files._connection.total_changes == changes and canonical_calls == 1
        assert binding[2].resources.status.connections == 1
        assert binding[2].resources.status.blobs == 0
        assert (
            identity_body(files.child("directory-upload", initialize=False), byte_limit=8192)
            == original
        )
    assert binding[2].resources.status.connections == binding[2].resources.status.blobs == 0
