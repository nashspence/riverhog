from __future__ import annotations

import os
import subprocess
import sys
import threading

import pytest
from riverhog_core import canonical_discovery_retirement as owner
from riverhog_core import retained_workspace_handles as handles

from tests.support.retained_resource import fault_native as fault_native
from tests.unit.test_canonical_discovery_child_reclamation import prepare_child
from tests.unit.test_canonical_discovery_metadata_reclamation import prepare_metadata
from tests.unit.test_canonical_discovery_retired_metadata import case as case
from tests.unit.test_canonical_discovery_retired_metadata import fixed_inputs as fixed_inputs
from tests.unit.test_canonical_discovery_retired_metadata import original as original
from tests.unit.test_canonical_discovery_retired_metadata import qualified as qualified

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX retained original namespace")


@pytest.mark.parametrize("target", ("root", "coordinator", "directory", "lease"))
@pytest.mark.parametrize(
    ("fault", "replacement_kind"),
    (
        ("after_open", None),
        ("stat_refusal", None),
        ("before_close", None),
        ("after_close", "unrelated"),
        ("after_close", "same-inode"),
    ),
)
def test_original_namespace_receives_native_outcomes_without_integer_retry(
    case, monkeypatch, fault_native, tmp_path, target, fault, replacement_kind
):
    native, _ = fault_native
    job, path = prepare_metadata(case)
    retained = owner.DiscoveryRetirementTurn(case.factory, job, case.root)
    names = {
        "root": case.root,
        "coordinator": ".coordinator",
        "directory": job.workspace_id,
        "lease": ".lease",
    }
    actual = handles.RetainedWorkspaceResource.acquire
    selected, original_descriptors = [], []

    def acquire(handle, name, **kwargs):
        if name == names[target]:
            selected.append(handle)
            handle.owner._test_fault(fault)
        result = actual(handle, name, **kwargs)
        if name == names[target]:
            original_descriptors.append(handle.fileno())
        return result

    replacement = None
    try:
        with monkeypatch.context() as patch:
            patch.setattr(handles, "Resource", native.Resource)
            patch.setattr(handles, "Borrow", native.Borrow)
            patch.setattr(handles.RetainedWorkspaceResource, "acquire", acquire)
            with pytest.raises((RuntimeError, OSError)):
                retained.advance(octet_budget=3)
        assert len(selected) == 1 and not retained.received
        chosen = selected[0]
        if fault == "after_close":
            assert chosen.owner.state == "received" and chosen.owner.metrics["close_calls"] == 1
            if target == "lease":
                assert not retained._custody.verify_current()
            (descriptor,) = original_descriptors
            with pytest.raises(OSError):
                os.fstat(descriptor)
            if replacement_kind == "unrelated":
                reopened = os.open(
                    tmp_path / "unrelated", os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600
                )
                os.write(reopened, b"unrelated original")
            else:
                original_path = {
                    "root": case.root,
                    "coordinator": case.root / ".coordinator",
                    "directory": path,
                    "lease": path / ".lease",
                }[target]
                flags = os.O_RDONLY | (os.O_DIRECTORY if target in {"root", "directory"} else 0)
                reopened = os.open(original_path, flags)
            if reopened != descriptor:
                os.dup2(reopened, descriptor)
                os.close(reopened)
            replacement = descriptor
            replacement_identity = os.fstat(replacement)
            if retained._custody is not None:
                assert not retained._custody.verify_current()
        elif fault == "before_close":
            assert chosen.owner.state == "live" and chosen.owner.metrics["close_calls"] == 0
        elif fault == "stat_refusal":
            assert chosen.owner.state == "rejected"
        else:
            assert chosen.owner.state == "live"
        retained.close()
        assert retained.received and not retained._handles._callbacks
        assert chosen.owner.state == "received" and chosen.owner.metrics["close_calls"] == 1
        if replacement is not None:
            assert os.fstat(replacement) == replacement_identity
        if retained._custody is not None:
            assert not retained._custody._held
        assert path.is_dir()
    finally:
        retained.close()
        if replacement is not None:
            os.close(replacement)


def test_original_namespace_executor_excludes_close_before_acquisition_returns(
    case, monkeypatch, fault_native
):
    native, _ = fault_native
    job, _ = prepare_metadata(case)
    retained = owner.DiscoveryRetirementTurn(case.factory, job, case.root)
    entered, release = threading.Event(), threading.Event()
    actual = handles.RetainedWorkspaceResource.acquire
    result, errors = [], []

    def acquire(handle, path, **kwargs):
        if path == case.root:
            entered.set()
            assert release.wait(10), "original namespace acquisition was not released"
        return actual(handle, path, **kwargs)

    def work():
        try:
            result.append(retained.advance(octet_budget=3))
        except BaseException as error:
            errors.append(error)

    with monkeypatch.context() as patch:
        patch.setattr(handles, "Resource", native.Resource)
        patch.setattr(handles, "Borrow", native.Borrow)
        patch.setattr(handles.RetainedWorkspaceResource, "acquire", acquire)
        worker = threading.Thread(target=work)
        worker.start()
        try:
            assert entered.wait(5)
            with pytest.raises(BlockingIOError, match="executor"):
                retained.close()
            assert not retained.received
        finally:
            release.set()
            worker.join(timeout=10)
        assert not worker.is_alive() and not errors and len(result) == 1
        assert retained.received and result[0].removed_octets == 3
    retained.close()


def test_unknown_original_namespace_preserves_dependency_and_refuses_reuse_in_owned_process(
    case, fault_native
):
    job, _ = prepare_metadata(case)
    _, extension = fault_native
    program = r"""
import importlib.util,json,os,sys
from pathlib import Path
from sqlalchemy.orm import sessionmaker
from riverhog_core.catalog_db import create_catalog_engine
from riverhog_core.catalog_discovery_validation_models import DiscoveryValidationJobRecord as Row
from riverhog_core.canonical_discovery_corpus_jobs import DiscoveryJob
from riverhog_core import canonical_discovery_retirement as owner
from riverhog_core import retained_workspace_handles as handles
spec=importlib.util.spec_from_file_location("_retained_resource_test._native",sys.argv[4])
native=importlib.util.module_from_spec(spec);spec.loader.exec_module(native)
handles.Resource,handles.Borrow=native.Resource,native.Borrow
engine=create_catalog_engine(sys.argv[1]);factory=sessionmaker(engine,expire_on_commit=False)
with factory() as session:job=DiscoveryJob.read(session.get(Row,sys.argv[3]))
retained=owner.DiscoveryRetirementTurn(factory,job,Path(sys.argv[2]))
actual=handles.RetainedWorkspaceResource.acquire
captured=[]
def acquire(original,path,**kwargs):
 result=actual(original,path,**kwargs)
 if path==".lease":
  captured.append((original,kwargs["parent"],original.fileno()))
  original.owner._test_fault("unknown_close_closed")
 return result
handles.RetainedWorkspaceResource.acquire=acquire
try:retained.advance(octet_budget=3)
except InterruptedError:pass
else:raise AssertionError("unknown original namespace became received")
original,parent,old=captured[0]
assert original.owner.state=="unknown" and not original.owner.received and not retained.received
assert parent.owner.state=="live" and parent.owner.metrics["children"]==1
assert retained._custody._held and not retained._custody.verify_current()
with factory() as session:
 current=session.get(Row,job.workspace_id)
 assert current is not None and current.version==job.version
other=os.open(Path(sys.argv[2])/job.workspace_id/".lease",os.O_RDONLY|os.O_NOFOLLOW)
if other!=old:os.dup2(other,old,inheritable=False);os.close(other)
try:
 for _ in range(17):
  try:retained.close()
  except RuntimeError:pass
  else:raise AssertionError("unknown original acquired a received outcome")
  os.fstat(old)
  assert not retained._custody.verify_current()
 assert original.owner.metrics["close_calls"]==1
 assert parent.owner.state=="live" and parent.owner.metrics["children"]==1
finally:os.close(old);engine.dispose()
print(json.dumps({"unknown":True,"original_dependency_retained":True,
                 "same_inode_grants_no_custody":True,"no_integer_retry":True}))
# Process exit cleans this isolated witness; it is no product receiving receipt.
"""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            program,
            str(case.engine.url),
            str(case.root),
            job.workspace_id,
            str(extension),
        ],
        capture_output=True,
        check=False,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr.decode()[-1000:]
    assert b'"unknown": true' in result.stdout
    assert b'"same_inode_grants_no_custody": true' in result.stdout


@pytest.mark.parametrize("target", ("prefix-root", "prefix-child", "routing-root", "metadata-file"))
@pytest.mark.parametrize(
    ("fault", "replacement_kind"),
    (
        ("after_open", None),
        ("stat_refusal", None),
        ("before_close", None),
        ("after_close", "unrelated"),
        ("after_close", "same-inode"),
    ),
)
def test_retired_child_namespace_preserves_original_parent_through_native_receiving(
    case, monkeypatch, fault_native, tmp_path, target, fault, replacement_kind
):
    native, _ = fault_native
    job, path, bindings = prepare_child(case, count=1)
    if target == "routing-root":
        first = owner.DiscoveryRetirementTurn(case.factory, job, case.root)
        try:
            job = first.advance(octet_budget=1024 * 1024).job
        finally:
            first.close()
        assert job.reclamation_phase == "child_directory"
    retained = owner.DiscoveryRetirementTurn(case.factory, job, case.root)
    original_path = path / "journals"
    if target == "metadata-file":
        original_path = path / "discovery.sqlite3"
        (path / "journals" / bindings[0].directory_id / "snapshot.sqlite3").write_bytes(b"x" * 17)
    if target == "prefix-child":
        original_path /= bindings[0].directory_id
    actual = handles.RetainedWorkspaceResource.acquire
    selected, descriptors, matching = [], [], []
    replacement = None

    def acquire(handle, name, **kwargs):
        wanted = (
            "discovery.sqlite3"
            if target == "metadata-file"
            else bindings[0].directory_id
            if target == "prefix-child"
            else "journals"
        )
        if name == wanted:
            matching.append(handle)
            if len(matching) == (2 if target == "routing-root" else 1):
                selected.append(handle)
                handle.owner._test_fault(fault)
        result = actual(handle, name, **kwargs)
        if handle in selected:
            descriptors.append(handle.fileno())
        return result

    try:
        with monkeypatch.context() as patch:
            patch.setattr(handles, "Resource", native.Resource)
            patch.setattr(handles, "Borrow", native.Borrow)
            patch.setattr(handles.RetainedWorkspaceResource, "acquire", acquire)
            with pytest.raises((RuntimeError, OSError)):
                retained.advance(octet_budget=3 if target == "metadata-file" else 1024 * 1024)
        assert len(selected) == 1 and not retained.received
        chosen = selected[0]
        metadata = retained._metadata
        assert metadata is not None
        if target != "metadata-file":
            assert metadata._db is not None
        if fault in {"before_close", "after_close"}:
            assert not metadata.verify_current()
        assert retained._custody.verify_current()
        assert metadata._namespace is retained._directory_owner
        if fault == "after_close":
            assert chosen.owner.state == "received"
            assert chosen.owner.metrics["close_calls"] == 1
            (descriptor,) = descriptors
            with pytest.raises(OSError):
                os.fstat(descriptor)
            if replacement_kind == "unrelated":
                reopened = os.open(
                    tmp_path / "unrelated-child", os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600
                )
                os.write(reopened, b"unrelated child namespace witness")
            else:
                reopened = os.open(
                    original_path,
                    os.O_RDONLY | (0 if target == "metadata-file" else os.O_DIRECTORY),
                )
            if reopened != descriptor:
                os.dup2(reopened, descriptor)
                os.close(reopened)
            replacement = descriptor
            replacement_identity = os.fstat(replacement)
        elif fault == "before_close":
            assert chosen.owner.state == "live" and chosen.owner.metrics["close_calls"] == 0
        elif target == "metadata-file":
            assert chosen.owner.state == ("rejected" if fault == "stat_refusal" else "live")
        else:
            # The retained child context receives acquisition/type-refusal
            # cleanup before propagating the original failure.
            assert chosen.owner.received and chosen.owner.metrics["close_calls"] == 1
        retained.close()
        assert retained.received and metadata._closed
        assert chosen.owner.received and chosen.owner.metrics["close_calls"] == 1
        assert not retained._custody._held
        if replacement is not None:
            assert os.fstat(replacement) == replacement_identity
        with case.factory() as session:
            assert owner.jobs.retire_discovery_job(session, job) == job
    finally:
        retained.close()
        if replacement is not None:
            os.close(replacement)


@pytest.mark.parametrize(
    "target",
    ("metadata-file", "prefix-child", "routing-root", "journals-root", "journals-coordinator"),
)
@pytest.mark.parametrize("fault", ("unknown_open", "unknown_close_open", "unknown_close_closed"))
def test_unknown_retired_storage_and_namespace_keep_original_dependencies_in_owned_process(
    case, fault_native, target, fault
):
    if target.startswith("journals-"):
        from tests.unit.test_canonical_discovery_root_reclamation import prepare_root

        job, _ = prepare_root(case)
        child_id = ""
    else:
        job, path, bindings = prepare_child(case, count=1)
        child_id = bindings[0].directory_id
        if target == "metadata-file":
            (path / "journals" / child_id / "snapshot.sqlite3").write_bytes(b"x" * 17)
        elif target == "routing-root":
            first = owner.DiscoveryRetirementTurn(case.factory, job, case.root)
            try:
                job = first.advance(octet_budget=1024 * 1024).job
            finally:
                first.close()
            assert job.reclamation_phase == "child_directory"
    _, extension = fault_native
    program = """
import importlib.util,json,os,sys
from pathlib import Path
from sqlalchemy.orm import sessionmaker
from riverhog_core.catalog_db import create_catalog_engine
from riverhog_core.catalog_discovery_validation_models import DiscoveryValidationJobRecord as Row
from riverhog_core.canonical_discovery_corpus_jobs import DiscoveryJob
from tests.support.validation_workspace import leased_workspace
from riverhog_core import canonical_discovery_retirement as owner
from riverhog_core import retained_workspace_handles as handles
spec=importlib.util.spec_from_file_location("_retained_resource_test._native",sys.argv[4])
native=importlib.util.module_from_spec(spec);spec.loader.exec_module(native)
handles.Resource,handles.Borrow=native.Resource,native.Borrow
engine=create_catalog_engine(sys.argv[1]);factory=sessionmaker(engine,expire_on_commit=False)
root=Path(sys.argv[2]);target,fault,child_id=sys.argv[5:8]
with factory() as session:job=DiscoveryJob.read(session.get(Row,sys.argv[3]))
journals=root/job.workspace_id/"journals"
journals_inode=journals.stat().st_ino
retained=owner.DiscoveryRetirementTurn(factory,job,root)
actual=handles.RetainedWorkspaceResource.acquire
captured=[];matching=[]
def acquire(original,path,**kwargs):
 parent=kwargs.get("parent")
 chosen=False
 if target=="metadata-file":chosen=path=="discovery.sqlite3"
 elif target=="prefix-child":chosen=path==child_id
 elif target=="journals-coordinator":
  chosen=(path==".coordinator" and parent is not None
          and os.fstat(parent.fileno()).st_ino==journals_inode)
 elif path=="journals":
  matching.append(original);chosen=len(matching)==(2 if target=="routing-root" else 1)
 if chosen:
  captured.append((original,parent,[]))
  original.owner._test_fault(fault)
 result=actual(original,path,**kwargs)
 if chosen:captured[-1][2].append(original.fileno())
 return result
handles.RetainedWorkspaceResource.acquire=acquire
try:retained.advance(octet_budget=3 if target=="metadata-file" else 1024*1024)
except (InterruptedError,RuntimeError):pass
else:raise AssertionError("indeterminate original resource became received")
assert len(captured)==1
original,parent,descriptors=captured[0]
assert original.owner.state=="unknown" and not original.owner.received
assert parent is not None and parent.owner.state=="live" and parent.owner.metrics["children"]>=1
assert not retained.received and retained._custody.verify_current()
assert retained._metadata is not None and not retained._metadata.verify_current()
with factory() as session:
 current=session.get(Row,job.workspace_id)
 assert current is not None and current.version==job.version
original_counts=original.owner.metrics.copy();dependencies=parent.owner.metrics["children"]
replacement=None
if fault=="unknown_close_closed":
 (old,)=descriptors
 try:os.fstat(old)
 except OSError:pass
 else:raise AssertionError("compiled closed-unknown witness did not close")
 if target=="metadata-file":path=root/job.workspace_id/"discovery.sqlite3";flags=os.O_RDONLY
 elif target=="prefix-child":path=journals/child_id;flags=os.O_RDONLY|os.O_DIRECTORY
 else:
  path=root/"unrelated-unknown-witness";path.write_bytes(b"unrelated");flags=os.O_RDONLY
 fresh=os.open(path,flags|os.O_NOFOLLOW)
 if fresh!=old:os.dup2(fresh,old,inheritable=False);os.close(fresh)
 replacement=old;identity=os.fstat(old)
try:
 for _ in range(17):
  try:retained.close()
  except RuntimeError:pass
  else:raise AssertionError("indeterminate resource acquired a received outcome")
  assert not retained.received and not original.owner.received
  assert parent.owner.state=="live" and parent.owner.metrics["children"]==dependencies
  assert original.owner.metrics==original_counts
  assert not retained._metadata.verify_current()
  if replacement is not None:assert os.fstat(replacement)==identity
 with leased_workspace(root,"f"*32,create=True) as independent:
  assert independent is not None
 with factory() as session:
  current=session.get(Row,job.workspace_id)
  assert current is not None and current.version==job.version
finally:
 if replacement is not None:os.close(replacement)
 engine.dispose()
print(json.dumps({"unknown":True,"original_dependencies_retained":True,
 "no_resource_replay":True,"no_catalog_completion":True,"independent_workspace_progressed":True}))
# Process exit disposes this isolated witness; it is no product receiving receipt.
"""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            program,
            str(case.engine.url),
            str(case.root),
            job.workspace_id,
            str(extension),
            target,
            fault,
            child_id,
        ],
        capture_output=True,
        check=False,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr.decode()[-1600:]
    assert b'"original_dependencies_retained": true' in result.stdout
    assert b'"no_catalog_completion": true' in result.stdout
    assert b'"independent_workspace_progressed": true' in result.stdout
