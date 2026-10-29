"""Crash and admission witnesses for the private prepared-byte store."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import time
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path

import pytest
from riverhog_client import _prepared_primary_posix as filesystem
from riverhog_client._prepared_primary_documents import (
    PrimaryScope,
    PrimarySession,
    primary_control_bytes,
)
from riverhog_client._prepared_primary_store import (
    PrimaryPreparation,
    PrimaryStore,
    PrimaryStoreLimits,
)
from riverhog_client._prepared_primary_types import PrimaryStateCapacity, PrimaryStateError
from riverhog_protocol import (
    ArtifactMemberIdentityDocument,
    CollectionArtifactProvenanceBindingDocument,
    CollectionUploadArtifactCustodyReceiptDocument,
    CollectionUploadCustodyObjectDocument,
    CollectionUploadProvenanceCustodyObjectDocument,
)

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX filesystem backend")

SCOPE = PrimaryScope(service="fixture-service", account="fixture-account")
LIMITS = PrimaryStoreLimits(
    pending_bytes=1024 * 1024,
    total_bytes=4 * 1024 * 1024,
    preparation_bytes=256 * 1024,
    control_bytes=8192,
)
CREATE = b'{"fixture":"exact create inputs"}'
REQUEST = b'{"fixture":"exact member retry inputs"}'
PAYLOAD = b"fixture member"
JOURNAL = b"exact retained journal octets"


def _member(number: int = 1):
    return ArtifactMemberIdentityDocument.model_validate(
        {
            "artifact_id": f"{number:064x}",
            "bytes": str(len(PAYLOAD)),
            "sha256": hashlib.sha256(PAYLOAD).hexdigest(),
        }
    )


def _author(member=None, *, journal=JOURNAL):
    member = member or _member()
    binding = CollectionArtifactProvenanceBindingDocument.model_validate(
        {
            "artifact_id": member.artifact_id,
            "journal": {
                "journal_id": "urn:uuid:aaaabbbb-1234-4234-8234-123456789abc",
                "through": {
                    "entry_id": "urn:uuid:aaaabbbb-4321-4321-8321-123456789abc",
                    "sequence": "0",
                    "json_sha256": "a" * 64,
                },
                "prefix_bytes": str(len(journal)),
                "prefix_sha256": hashlib.sha256(journal).hexdigest(),
            },
            "delivery_association_id": "urn:uuid:bbbbaaaa-1234-4234-8234-123456789abc",
        }
    )
    # Store tests own octet retention; Native authorship is qualified by its owner.
    return PrimaryPreparation(
        b"original selection", b"original graph", journal, b"exact decision", binding, None
    )


def _receipt(prepared, member=None):
    member = prepared.member if member is None else member
    return CollectionUploadArtifactCustodyReceiptDocument.seal(
        collection_id=42,
        artifact_id=member.artifact_id,
        bytes=member.bytes,
        sha256=member.sha256,
        primary=prepared.primary,
        completion_requirement_sha256=None,
        archive_objects=(
            CollectionUploadCustodyObjectDocument(
                volume_id="pack-" + "0" * 64,
                sealed_receipt_sha256="a" * 64,
            ),
        ),
        provenance_objects=(
            CollectionUploadProvenanceCustodyObjectDocument(
                object_id="fixture-primary",
                relative_path="provenance/payloads/fixture.bin.age",
                plaintext_bytes=str(prepared.primary.journal.prefix_bytes),
                plaintext_sha256=prepared.primary.journal.prefix_sha256,
                sealed_receipt_sha256="b" * 64,
            ),
        ),
    )


def _open(root, *, initialize=False, limits=LIMITS, scope=SCOPE, storage_factory=None):
    store = PrimaryStore(
        root,
        allocation_id="existing-app-allocation",
        scope=scope,
        limits=limits,
        initialize=initialize,
        **({"storage_factory": storage_factory} if storage_factory is not None else {}),
    )
    if initialize:
        assert store.session_for(CREATE) is None
        store.bind_session(
            PrimarySession(
                allocation_sha256=hashlib.sha256(
                    primary_control_bytes(store.allocation)
                ).hexdigest(),
                create_request={
                    "bytes": str(len(CREATE)),
                    "sha256": hashlib.sha256(CREATE).hexdigest(),
                },
                collection_id="42",
                delivery_context_id="urn:uuid:ccccaaaa-1234-4234-8234-123456789abc",
                server_construction_identity_sha256="a" * 64,
                attribution_construction_identity_sha256="b" * 64,
                completion_requirement_sha256=None,
            )
        )
    return store


def _no_author(_admission):
    pytest.fail("an already selected primary was authored again")


def test_cold_replay_uses_the_original_winner_and_compares_before_authoring(tmp_path):
    root = tmp_path.resolve() / "state"
    with _open(root, initialize=True) as store:
        original = store.prepare_or_load(_member(), REQUEST, lambda _limit: _author())
        expected_usage = store.usage
        assert expected_usage.members == 1
    with _open(root) as store:
        assert store.session_for(CREATE) == store.session
        loaded = store.prepare_or_load(_member(), REQUEST, _no_author)
        assert loaded == original
        assert store.usage == expected_usage
        with store.byte_reader(loaded, "journal") as stream:
            assert stream.read() == JOURNAL
        with pytest.raises(PrimaryStateError, match="retry inputs changed"):
            store.prepare_or_load(_member(), REQUEST + b"changed", _no_author)
        with pytest.raises(PrimaryStateError, match="creation inputs changed"):
            store.session_for(CREATE + b"changed")
        assert store.usage == expected_usage
    with pytest.raises(PrimaryStateError, match="scope"):
        _open(root, scope=PrimaryScope(service="other-service", account=SCOPE.account))


def test_explicit_initial_creation_does_not_adopt_an_ambiguous_remote_outcome(tmp_path):
    root = tmp_path.resolve() / "state"
    with PrimaryStore(
        root, allocation_id="allocation", scope=SCOPE, limits=LIMITS, initialize=True
    ) as store:
        assert store.session_for(CREATE) is None
    with PrimaryStore(
        root, allocation_id="allocation", scope=SCOPE, limits=LIMITS, initialize=False
    ) as store:
        with pytest.raises(PrimaryStateError, match="uncertain outcome"):
            store.session_for(CREATE)
    with pytest.raises(FileExistsError):
        PrimaryStore(root, allocation_id="allocation", scope=SCOPE, limits=LIMITS, initialize=True)
    with pytest.raises(FileNotFoundError):
        _open(tmp_path.resolve() / "missing")
    (root / "allocation").write_text('{"format":"unknown"}')
    with pytest.raises(ValueError):
        PrimaryStore(root, allocation_id="allocation", scope=SCOPE, limits=LIMITS, initialize=False)


@pytest.mark.parametrize("corruption", ["missing", "truncated", "changed"])
def test_committed_missing_or_corrupt_preimages_never_reauthor(tmp_path, corruption):
    root = tmp_path.resolve() / "state"
    with _open(root, initialize=True) as store:
        store.prepare_or_load(_member(), REQUEST, lambda _limit: _author())
        with store._member(str(_member().artifact_id), create=False) as files:
            path = files.root / "journal"
    if corruption == "missing":
        path.unlink()
    elif corruption == "truncated":
        path.write_bytes(JOURNAL[:3])
    else:
        path.write_bytes(b"x" * len(JOURNAL))
    with _open(root) as store:
        with pytest.raises(PrimaryStateError):
            store.prepare_or_load(_member(), REQUEST, _no_author)


def test_capacity_defers_new_preparation_but_reserved_custody_and_reclaim_progress(tmp_path):
    root = tmp_path.resolve() / "state"
    size = len(REQUEST) + sum(
        len(raw) for raw in (b"original selection", b"original graph", JOURNAL, b"exact decision")
    )
    limits = replace(LIMITS, pending_bytes=size)
    with _open(root, initialize=True, limits=limits) as store:
        record = store.prepare_or_load(_member(), REQUEST, lambda _limit: _author())
        assert store.usage.pending_bytes == size
        before = store.usage
        with pytest.raises(PrimaryStateCapacity, match="admission exhausted"):
            store.prepare_or_load(_member(2), REQUEST, _no_author)
        assert store.usage == before
        assert not store.reclaim(_member())
        record = store.accept_custody(record, _receipt(record.prepared))
        assert record.custody is not None and not record.compact
        assert not store.reclaim(_member())
        assert store.usage.pending_bytes == size
        with pytest.raises(ValueError):
            store.accept_decision(record, b"another decision")
        record = store.accept_decision(record, b"exact decision")
        assert record.compact
        assert store.reclaim(_member())
        assert store.reclaim(_member())
        assert store.usage.pending_bytes == 0
        assert store.usage.control_bytes == limits.member_reservation
        with pytest.raises(FileNotFoundError):
            with store.byte_reader(record, "journal"):
                pytest.fail("a reclaimed journal was exposed")
        store.prepare_or_load(_member(2), REQUEST, lambda _limit: _author(_member(2)))
    with _open(root, limits=limits) as store:
        assert store.prepare_or_load(_member(), REQUEST, _no_author).compact
        assert store.reclaim(_member())
        assert store.usage.members == 2
        assert store.usage.pending_bytes == size
        with pytest.raises(PrimaryStateError, match="retry inputs changed"):
            store.prepare_or_load(_member(), b"new input", _no_author)


_CHILD = """
import json,os,sys
from pathlib import Path
from riverhog_client._prepared_primary_store import (
    PrimaryStore,PrimaryStoreLimits,PrimaryPreparation)
from riverhog_client._prepared_primary_documents import PrimaryScope
from riverhog_client._prepared_primary_posix import PrimaryFiles
from riverhog_protocol import ArtifactMemberIdentityDocument
root=Path(sys.argv[1]); spec=json.loads(sys.argv[2]); boundary=sys.argv[3]
store=PrimaryStore(root,allocation_id='existing-app-allocation',scope=PrimaryScope(**spec['scope']),
    limits=PrimaryStoreLimits(**spec['limits']),initialize=False)
member=ArtifactMemberIdentityDocument.model_validate(spec['member'])
put=PrimaryFiles.put
remove=PrimaryFiles.remove
closing_intent_writes=0
link=os.link
replace_file=os.replace
unlink=os.unlink
def crash_link(source,target,*args,**kwargs):
    if "candidate-"+target==boundary: os._exit(37)
    result=link(source,target,*args,**kwargs)
    if "linked-"+target==boundary: os._exit(37)
    return result
def crash_replace(source,target,*args,**kwargs):
    if "candidate-"+target==boundary: os._exit(37)
    return replace_file(source,target,*args,**kwargs)
os.link=crash_link
os.replace=crash_replace
def crash_unlink(name,*args,**kwargs):
    result=unlink(name,*args,**kwargs)
    if "unlinked-"+name==boundary: os._exit(37)
    return result
os.unlink=crash_unlink
def crash_put(self,name,*args,**kwargs):
    global closing_intent_writes
    result=put(self,name,*args,**kwargs)
    if name=="closing-intent":
        closing_intent_writes+=1
        if boundary=="closing-intent-ready" and closing_intent_writes==2: os._exit(37)
    if name==boundary: os._exit(37)
    return result
def crash_remove(self,name):
    remove(self,name)
    if 'remove-'+name==boundary: os._exit(37)
PrimaryFiles.put=crash_put
PrimaryFiles.remove=crash_remove
if spec['operation']=='prepare':
    value=spec['authored']
    store.prepare_or_load(member,spec['request'].encode(),lambda _limit:PrimaryPreparation(
        value['observation'].encode(),value['graph'].encode(),value['journal'].encode(),
        value['decision'].encode(),value['primary'],None))
elif spec['operation']=='reclaim':
    store.reclaim(member)
elif spec['operation']=='custody':
    from riverhog_client._prepared_primary_closing import advance_custody
    from riverhog_protocol import CollectionUploadArtifactCustodyReceiptDocument
    def settle(item):
        store.accept_custody(store.load_controls(item),
            CollectionUploadArtifactCustodyReceiptDocument.model_validate(spec['receipt']))
    advance_custody(store,limit=1,settle=settle)
else:
    from riverhog_client._prepared_primary_closing import freeze_inventory,advance_closing
    if spec['operation']=='freeze':
        freeze_inventory(store,(member,))
    else:
        def settle(item): store.accept_decision(store.load(item),b'exact decision')
        advance_closing(store,limit=1,settle=settle)
raise RuntimeError('crash boundary was not reached')
"""


def _crash(root, boundary, *, operation="prepare"):
    authored = _author()
    spec = {
        "operation": operation,
        "receipt": _receipt(authored, _member()).model_dump(mode="json"),
        "scope": SCOPE.model_dump(mode="json"),
        "limits": {
            key: getattr(LIMITS, key)
            for key in ("pending_bytes", "total_bytes", "preparation_bytes", "control_bytes")
        },
        "member": _member().model_dump(mode="json"),
        "request": REQUEST.decode(),
        "authored": {
            "observation": authored.observation.decode(),
            "graph": authored.graph.decode(),
            "journal": authored.journal.decode(),
            "decision": authored.decision.decode(),
            "primary": authored.primary.model_dump(mode="json"),
        },
    }
    result = subprocess.run(
        [sys.executable, "-c", _CHILD, str(root), json.dumps(spec), boundary],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 37, result.stderr


@pytest.mark.parametrize(
    "boundary",
    [
        "request",
        "graph",
        "candidate-journal",
        "candidate-prepared",
        "prepared",
        "candidate-usage",
        "usage",
    ],
)
def test_process_exit_recovers_only_the_active_candidate_or_durable_winner(tmp_path, boundary):
    root = tmp_path.resolve() / "state"
    with _open(root, initialize=True):
        pass
    _crash(root, boundary)
    with _open(root) as store:
        if boundary in ("request", "graph", "candidate-journal", "candidate-prepared"):
            assert store.load(_member()) is None
            assert store.usage.members == store.usage.pending_bytes == 0
            record = store.prepare_or_load(_member(), REQUEST, lambda _limit: _author())
        else:
            record = store.prepare_or_load(_member(), REQUEST, _no_author)
        assert store.usage.members == 1
        with store.byte_reader(record, "journal") as stream:
            assert stream.read() == JOURNAL
        assert not (root / "transition").exists()


@pytest.mark.parametrize(
    "boundary",
    [
        "remove-request",
        "remove-journal",
        "candidate-reclaimed",
        "reclaimed",
        "candidate-usage",
        "usage",
    ],
)
@pytest.mark.parametrize("truncated", [False, True])
def test_process_exit_during_reclaim_never_subtracts_twice_or_requires_released_bytes(
    tmp_path, boundary, truncated
):
    root = tmp_path.resolve() / "state"
    with _open(root, initialize=True) as store:
        record = store.prepare_or_load(_member(), REQUEST, lambda _limit: _author())
        store.accept_custody(record, _receipt(record.prepared))
        store.accept_decision(record, b"exact decision")
        if truncated:
            with store._member(str(_member().artifact_id), create=False) as files:
                (files.root / "journal").write_bytes(JOURNAL[:1])
    _crash(root, boundary, operation="reclaim")
    with _open(root) as store:
        record = store.prepare_or_load(_member(), REQUEST, _no_author)
        assert record.compact
        assert store.reclaim(_member())
        first = store.usage
        assert first.pending_bytes == 0 and first.members == 1
        assert store.reclaim(_member()) and store.usage == first


def test_failed_local_publication_never_selects_unflushed_bytes(tmp_path, monkeypatch):
    root = tmp_path.resolve() / "state"
    with _open(root, initialize=True) as store:
        sync = filesystem.os.fsync

        def fail(_descriptor):
            raise OSError("injected full disk/fsync error")

        with monkeypatch.context() as patch:
            patch.setattr(filesystem.os, "fsync", fail)
            with pytest.raises(OSError):
                store.prepare_or_load(_member(), REQUEST, lambda _limit: _author())
        assert filesystem.os.fsync is sync
        assert store.load(_member()) is None
        store.prepare_or_load(_member(), REQUEST, lambda _limit: _author())


def test_point_lookup_and_compact_reopen_do_not_enumerate_member_backlog(tmp_path, monkeypatch):
    root = tmp_path.resolve() / "state"
    with _open(root, initialize=True) as store:
        for number in range(1, 18):
            member = _member(number)
            store.prepare_or_load(member, REQUEST, lambda _limit, member=member: _author(member))

        def no_scan(*_args, **_kwargs):
            pytest.fail("point recovery enumerated the construction backlog")

        with monkeypatch.context() as patch:
            patch.setattr(os, "scandir", no_scan)
            patch.setattr(Path, "iterdir", no_scan)
            assert store.prepare_or_load(
                _member(17), REQUEST, _no_author
            ).prepared.member == _member(17)
            assert store.load(_member(99)) is None


@pytest.fixture(scope="module")
def native_observation(tmp_path_factory):
    from riverhog_provenance import BoundedSourceObserver, BytesSource

    state = tmp_path_factory.mktemp("publisher-native")
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("RIVERHOG_PROVENANCE_STATE_HOME", str(state / "identity"))
        yield BoundedSourceObserver().observe(BytesSource(PAYLOAD))


class _PublicationApi:
    def __init__(self):
        self.journals = {}
        self.binding = None
        self.member_exists = False
        self.before_bind = False
        self.after_decision = False
        self.decision_calls = []
        self.return_another_decision = False

    def get_collection_upload_session(self, _collection):
        return {
            "collection_id": "42",
            "state": "open",
            "delivery_context_id": "urn:uuid:ccccaaaa-1234-4234-8234-123456789abc",
            "construction_identity_sha256": "a" * 64,
        }

    def get_collection_upload_session_artifact(self, _collection, _artifact):
        from riverhog_protocol.errors import NotFound

        if not self.member_exists:
            raise NotFound("member is absent")
        return _member().model_dump(mode="json")

    def get_collection_upload_session_artifact_provenance_binding(self, *_args):
        from riverhog_protocol.errors import NotFound

        if self.binding is None:
            raise NotFound("binding is absent")
        return self.binding

    def upload_collection_upload_session_provenance_journal(
        self, _collection, journal_id, *, content, byte_count, sha256
    ):
        raw = b"".join(content)
        assert len(raw) == byte_count and hashlib.sha256(raw).hexdigest() == sha256
        assert self.journals.setdefault(journal_id, raw) == raw

    def bind_collection_upload_session_artifact_provenance(self, _collection, batch):
        if self.before_bind:
            self.before_bind = False
            raise RuntimeError("lost before binding")
        assert self.binding in (None, batch.bindings[0])
        self.binding = batch.bindings[0]
        return batch

    def set_collection_upload_session_materialization_decisions(self, _collection, batch):
        self.decision_calls.append(batch)
        if self.after_decision:
            self.after_decision = False
            raise RuntimeError("lost decision acknowledgement")
        if self.return_another_decision:
            return batch.model_copy(update={"decisions": []})
        return batch


def _publish(publisher, observation, **changed):
    from riverhog_client.canonical_production import ProducerAttribution

    options = {
        "member": _member(),
        "attribution": ProducerAttribution(
            "fixture", "fixture/v1", "1", "event", "fixture", {}, "b" * 64
        ),
        "observation": observation,
        "automatic_observation": _no_author,
        "materialization_hint": None,
        "allow_missing_materialization_hint": True,
    }
    options.update(changed)
    return publisher.prepare(**options)


def test_native_replay_reuses_staged_bytes_after_bind_and_decision_acknowledgement_loss(
    tmp_path, native_observation
):
    from riverhog_client._prepared_primary_publisher import (
        DeclaredPrimaryObservation,
        PrimaryPublisher,
    )

    root = tmp_path.resolve() / "state"
    api = _PublicationApi()
    observations = []

    def observe():
        observations.append("observed")
        return native_observation

    lazy = DeclaredPrimaryObservation(
        "fixture-observer/v1",
        {"source_identity": "fixture-source"},
        native_observation.catalog.checkpoint_sha256(),
        observe,
    )
    with _open(root, initialize=True) as store:
        publisher = PrimaryPublisher(api, store)
        original = _publish(publisher, lazy)
        api.member_exists = True
        api.before_bind = True
        with pytest.raises(RuntimeError, match="before binding"):
            publisher.replay(original)
        assert len(api.journals) == 1
        assert store.load(_member()).decision_accepted is None
    lazy = replace(lazy, factory=_no_author)
    with _open(root) as store:
        publisher = PrimaryPublisher(api, store)
        loaded = _publish(publisher, lazy)
        assert loaded.prepared == original.prepared
        api.after_decision = True
        with pytest.raises(RuntimeError, match="decision acknowledgement"):
            publisher.replay(loaded)
        assert store.load(_member()).decision_accepted is None
    with _open(root) as store:
        publisher = PrimaryPublisher(api, store)
        loaded = _publish(publisher, lazy)
        replayed = publisher.replay(loaded)
        assert replayed.decision_accepted is not None
        assert len(api.journals) == len(observations) == 1
        assert api.decision_calls[0] == api.decision_calls[1]
        assert publisher.replay(replayed) == replayed
        assert len(api.decision_calls) == 2


@pytest.mark.parametrize(
    "changed",
    [
        "mode",
        "source-context",
        "producer-version",
        "output",
        "omission",
        "hint",
        "lazy-configuration",
        "supplied-graph",
    ],
)
def test_complete_native_retry_descriptor_rejects_semantic_changes_before_publication(
    tmp_path, native_observation, changed
):
    from riverhog_client._prepared_primary_publisher import (
        DeclaredPrimaryObservation,
        PrimaryPublisher,
    )
    from riverhog_client.canonical_production import ProducerAttribution
    from riverhog_provenance import BoundedSourceObserver, BytesSource

    root = tmp_path.resolve() / "state"
    api = _PublicationApi()
    lazy = DeclaredPrimaryObservation(
        "fixture-observer/v1",
        {"source_identity": "fixture-source"},
        native_observation.catalog.checkpoint_sha256(),
        lambda: native_observation,
    )
    observation = native_observation if changed == "supplied-graph" else lazy
    with _open(root, initialize=True) as store:
        publisher = PrimaryPublisher(api, store)
        options = (
            {"materialization_hint": ("original",), "allow_missing_materialization_hint": False}
            if changed in ("hint", "omission")
            else {}
        )
        original = _publish(publisher, observation, **options)
        if changed == "mode":
            observation = None
        elif changed == "lazy-configuration":
            observation = replace(
                lazy, configuration={"source_identity": "another-source"}, factory=_no_author
            )
        elif changed == "supplied-graph":
            observation = BoundedSourceObserver().observe(BytesSource(PAYLOAD))
        elif changed == "source-context":
            options["attribution"] = ProducerAttribution(
                "fixture",
                "fixture/v1",
                "1",
                "event",
                "fixture",
                {"changed": "semantic input"},
                "b" * 64,
            )
        elif changed == "producer-version":
            options["attribution"] = ProducerAttribution(
                "fixture", "fixture/v1", "2", "event", "fixture", {}, "b" * 64
            )
        elif changed == "output":
            options["output_id"] = "another-output"
        elif changed == "omission":
            options["allow_missing_materialization_hint"] = True
            options["materialization_hint"] = None
        else:
            options["materialization_hint"] = ("another",)
        with pytest.raises(PrimaryStateError, match="retry inputs changed"):
            _publish(publisher, observation, **options)
        assert store.load(_member()).prepared == original.prepared
        assert api.journals == {} and api.decision_calls == []


def test_remote_member_without_local_intent_is_not_automatically_adopted(
    tmp_path, native_observation
):
    from riverhog_client._prepared_primary_publisher import PrimaryPublisher

    api = _PublicationApi()
    api.member_exists = True
    with _open(tmp_path.resolve() / "state", initialize=True) as store:
        with pytest.raises(PrimaryStateError, match="no original local preparation"):
            _publish(PrimaryPublisher(api, store), native_observation)
        assert store.usage.members == 0 and not api.journals


def test_exact_binding_and_decision_response_are_independent_requirements(
    tmp_path, native_observation
):
    from riverhog_client._prepared_primary_publisher import PrimaryPublisher

    api = _PublicationApi()
    with _open(tmp_path.resolve() / "state", initialize=True) as store:
        publisher = PrimaryPublisher(api, store)
        record = _publish(publisher, native_observation)
        api.binding = record.prepared.primary.model_copy(
            update={"delivery_association_id": "urn:uuid:ffffffff-1234-4234-8234-123456789abc"}
        )
        with pytest.raises(PrimaryStateError, match="entire retained binding"):
            publisher.replay(record)
        assert not api.journals and not api.decision_calls
        api.binding = None
        api.return_another_decision = True
        with pytest.raises(PrimaryStateError, match="retained decision"):
            publisher.replay(record)
        assert store.load(_member()).decision_accepted is None


def test_an_author_cannot_reenter_and_discard_its_own_active_transition(tmp_path):
    from riverhog_client._prepared_primary_types import PrimaryStateBusy

    with _open(tmp_path.resolve() / "state", initialize=True) as store:

        def reenter(_limit):
            _ = store.usage
            pytest.fail("active preparation permitted recovery reentry")

        with pytest.raises(PrimaryStateBusy, match="reenter"):
            store.prepare_or_load(_member(), REQUEST, reenter)
        assert store.usage.members == 0
        store.prepare_or_load(_member(), REQUEST, lambda _limit: _author())


class _CreationApi(_PublicationApi):
    def __init__(self):
        super().__init__()
        self.create_calls = 0
        self.get_calls = 0
        self.tag_calls = 0
        self.tags = set()
        self.resumed = False
        self.lose_create_ack = False
        self.lose_tag_ack = False
        self.read_denied = False
        self.finalized = False

    def create_or_resume_collection_upload_session(self, key, **options):
        self.create_calls += 1
        assert options["custody_mode"] == "custody-transfer"
        self.tags.update(options["tags"])
        if self.lose_create_ack:
            raise RuntimeError("lost first-create response")
        return {**super().get_collection_upload_session(42), "resumed": self.resumed}

    def get_collection_upload_session(self, collection):
        from riverhog_protocol.errors import Forbidden

        self.get_calls += 1
        if self.read_denied:
            raise Forbidden("current principal cannot read the bound session")
        value = super().get_collection_upload_session(collection)
        if self.finalized:
            value["state"] = "finalized"
        return value

    def add_collection_upload_session_tags(self, _collection, tags):
        assert tags
        self.tag_calls += 1
        self.tags.update(tags)
        if self.lose_tag_ack:
            self.lose_tag_ack = False
            raise RuntimeError("lost initial-tag response")
        return {}


def _unbound(root):
    return PrimaryStore(
        root,
        allocation_id="existing-app-allocation",
        scope=SCOPE,
        limits=LIMITS,
        initialize=True,
    )


def _create(api, store, **changes):
    from riverhog_client._prepared_primary_session import open_primary_session
    from riverhog_client.canonical_production import ProducerAttribution

    options = {
        "attribution": ProducerAttribution(
            "fixture",
            "fixture/v1",
            "1",
            "event",
            "fixture",
            {},
            hashlib.sha256(b"event").hexdigest(),
        ),
        "idempotency_key": None,
        "archive_store": None,
        "use_cache": None,
        "copy_to": None,
        "description": None,
        "tags": (),
        "event_context": None,
    }
    options.update(changes)
    return open_primary_session(api, store, **options)


def test_bound_creation_uses_current_point_authority_and_never_creates_again(tmp_path):
    from riverhog_protocol.errors import Forbidden

    root = tmp_path.resolve() / "state"
    api = _CreationApi()
    with _unbound(root) as store:
        session, attribution = _create(api, store)
        assert attribution.construction_identity == hashlib.sha256(b"event").hexdigest()
        assert store.session.server_construction_identity_sha256 == "a" * 64
        assert store.session.attribution_construction_identity_sha256 != "a" * 64
        assert session["collection_id"] == "42" and api.create_calls == 1
        assert session["resumed"] is False
    with _open(root) as store:
        assert _create(api, store)[0]["resumed"] is True
        assert api.create_calls == api.get_calls == 1 and api.tag_calls == 0
        api.read_denied = True
        with pytest.raises(Forbidden):
            _create(api, store)
        assert api.create_calls == 1
        api.read_denied = False
        api.finalized = True
        session, _ = _create(api, store)
        assert session["state"] == "finalized" and api.tag_calls == 0


@pytest.mark.parametrize("changed", ["description", "tags", "event_context", "copy_to", "producer"])
def test_creation_input_change_conflicts_before_any_remote_call(tmp_path, changed):
    from riverhog_client.canonical_production import ProducerAttribution

    api = _CreationApi()
    with _unbound(tmp_path.resolve() / "state") as store:
        _create(api, store)
        if changed == "producer":
            changes = {
                "attribution": ProducerAttribution(
                    "fixture",
                    "fixture/v1",
                    "2",
                    "event",
                    "fixture",
                    {},
                    hashlib.sha256(b"event").hexdigest(),
                )
            }
        elif changed == "tags":
            changes = {"tags": ("changed",)}
        elif changed == "copy_to":
            changes = {"copy_to": ("archive",)}
        elif changed == "description":
            changes = {"description": "changed"}
        else:
            changes = {"event_context": {"changed": True}}
        with pytest.raises(PrimaryStateError, match="creation inputs changed"):
            _create(api, store, **changes)
        assert api.create_calls == 1 and api.get_calls == api.tag_calls == 0


@pytest.mark.parametrize("uncertain", ["lost-create", "resumed-without-header"])
def test_missing_accepted_creation_header_is_an_explicit_reconciliation_boundary(
    tmp_path, uncertain
):
    api = _CreationApi()
    api.lose_create_ack = uncertain == "lost-create"
    api.resumed = uncertain == "resumed-without-header"
    root = tmp_path.resolve() / "state"
    with _unbound(root) as store:
        with pytest.raises((RuntimeError, PrimaryStateError)):
            _create(api, store)
    with _open(root) as store:
        with pytest.raises(PrimaryStateError, match="uncertain outcome"):
            _create(api, store)
        assert api.create_calls == 1 and api.tag_calls == api.get_calls == 0


def test_lost_initial_tags_replays_only_under_the_original_accepted_header(tmp_path):
    root = tmp_path.resolve() / "state"
    api = _CreationApi()
    tags = tuple(f"fixture/{number:04d}" for number in range(205))
    api.lose_tag_ack = True
    with _unbound(root) as store:
        with pytest.raises(RuntimeError, match="initial-tag response"):
            _create(api, store, tags=tags)
        assert store.session.collection_id == 42
    with _open(root) as store:
        _create(api, store, tags=tags)
        assert api.tags == set(tags) and api.create_calls == 1 and api.get_calls == 1


def test_execution_attribution_and_completion_survive_lost_ack_and_finalized_resume(tmp_path):
    from riverhog_client.canonical_production import ProducerAttribution
    from riverhog_protocol.collection_completion import (
        COMPLETION_REQUIRED_RECORD_KINDS,
        CollectionCompletionRequirementDocument,
    )

    requirement = CollectionCompletionRequirementDocument(
        execution_id="1" * 64,
        execution_envelope_sha256="2" * 64,
        controller_evidence_sha256="3" * 64,
        record_kinds=tuple(sorted(COMPLETION_REQUIRED_RECORD_KINDS)),
    )
    attribution = ProducerAttribution(
        "fixture",
        "fixture/v1",
        "1",
        "event",
        "fixture",
        {},
        hashlib.sha256(b"event").hexdigest(),
        requirement,
    )
    api = _CreationApi()
    accepted = []

    def completion(_collection, value):
        accepted.append(value)
        if len(accepted) == 1:
            raise RuntimeError("lost completion acknowledgement")
        return value

    api.set_collection_upload_session_completion_requirement = completion
    root = tmp_path.resolve() / "state"
    with _unbound(root) as store:
        with pytest.raises(RuntimeError, match="completion acknowledgement"):
            _create(api, store, attribution=attribution)
        assert store.session.attribution_construction_identity_sha256 == "a" * 64
        assert store.session.completion_requirement_sha256 == requirement.identity
    with _open(root) as store:
        _session, resumed = _create(api, store, attribution=attribution)
        assert resumed.construction_identity == "a" * 64
        assert accepted == [requirement, requirement] and api.create_calls == 1
        api.finalized = True
        session, resumed = _create(api, store, attribution=attribution)
        assert session["state"] == "finalized" and resumed.construction_identity == "a" * 64
        assert len(accepted) == 2


def test_closing_freezes_the_exact_owner_inventory_and_replays_only_unacknowledged_members(
    tmp_path,
):
    from riverhog_client._prepared_primary_closing import advance_closing, freeze_inventory

    root = tmp_path.resolve() / "state"
    inventory = tuple(_member(number) for number in range(1, 4))
    with _open(root, initialize=True) as store:
        for member in inventory:
            store.prepare_or_load(member, REQUEST, lambda _limit, member=member: _author(member))
        closing = freeze_inventory(store, inventory)
        assert closing.after_member == 0 and store.usage.inventory_selected
        with pytest.raises(PrimaryStateError, match="closing allocation"):
            store.prepare_or_load(_member(4), REQUEST, _no_author)

        def lost_cursor(member):
            store.accept_decision(store.load(member), b"exact decision")
            raise RuntimeError("lost closure cursor")

        with pytest.raises(RuntimeError, match="closure cursor"):
            advance_closing(store, limit=1, settle=lost_cursor)
        assert store.load(inventory[0]).decision_accepted is not None
        assert not (root / "finish-ready").exists()

    def no_inventory():
        pytest.fail("a frozen owner inventory was traversed again")
        yield

    with _open(root) as store:
        assert freeze_inventory(store, no_inventory()) == closing
        calls = []

        def settle(member):
            calls.append(member.artifact_id)
            store.accept_decision(store.load(member), b"exact decision")

        assert not advance_closing(store, limit=1, settle=settle)
        assert calls == []
        assert not advance_closing(store, limit=1, settle=settle)
        assert advance_closing(store, limit=1, settle=settle)
        assert calls == [inventory[1].artifact_id, inventory[2].artifact_id]
        assert all(store.load(member).custody is None for member in inventory)
        assert advance_closing(
            store, limit=1, settle=lambda _member: pytest.fail("finished prefix replay")
        )
        retained_bytes = store.usage.inventory_bytes
        record = store.load(inventory[0])
        store.accept_custody(record, _receipt(record.prepared))
        assert store.reclaim(inventory[0])
        assert store.usage.inventory_bytes == retained_bytes and store.usage.inventory_selected


@pytest.mark.parametrize("invalid", ["duplicate", "omitted", "unknown"])
def test_closing_never_omits_or_replaces_allocated_preparations(tmp_path, invalid):
    from riverhog_client._prepared_primary_closing import freeze_inventory

    inventory = tuple(_member(number) for number in range(1, 4))
    with _open(tmp_path.resolve() / "state", initialize=True) as store:
        for member in inventory:
            store.prepare_or_load(member, REQUEST, lambda _limit, member=member: _author(member))
        if invalid == "duplicate":
            attempted = (inventory[0], inventory[0], inventory[2])
        elif invalid == "omitted":
            attempted = inventory[:-1]
        else:
            attempted = (inventory[0], inventory[1], _member(4))
        with pytest.raises(PrimaryStateError):
            freeze_inventory(store, attempted)
        with pytest.raises(PrimaryStateError, match="closing allocation"):
            store.prepare_or_load(_member(4), REQUEST, _no_author)
        assert freeze_inventory(store, inventory).members == 3


@pytest.mark.parametrize(
    "boundary",
    [
        "closing-intent",
        "candidate-inventory",
        "inventory",
        "closing-intent-ready",
        "closing",
        "candidate-usage",
        "usage",
    ],
)
def test_process_exit_during_inventory_freeze_never_omits_a_member_or_invents_finish_ready(
    tmp_path, boundary
):
    from riverhog_client._prepared_primary_closing import advance_closing, freeze_inventory

    root = tmp_path.resolve() / "state"
    with _open(root, initialize=True) as store:
        store.prepare_or_load(_member(), REQUEST, lambda _limit: _author())
    _crash(root, boundary, operation="freeze")
    with _open(root) as store:
        closing = freeze_inventory(store, (_member(),))
        assert closing.members == 1 and store.usage.inventory_bytes == closing.inventory.bytes
        assert not (root / "finish-ready").exists()

        def settle(member):
            store.accept_decision(store.load(member), b"exact decision")

        assert advance_closing(store, limit=1, settle=settle)
        assert store.load(_member()).custody is None


@pytest.mark.parametrize("boundary", ["closing", "finish-ready"])
def test_process_exit_after_final_decision_or_finish_barrier_resumes_without_reauthoring(
    tmp_path, boundary
):
    from riverhog_client._prepared_primary_closing import advance_closing, freeze_inventory

    root = tmp_path.resolve() / "state"
    with _open(root, initialize=True) as store:
        store.prepare_or_load(_member(), REQUEST, lambda _limit: _author())
        freeze_inventory(store, (_member(),))
    _crash(root, boundary, operation="advance")
    with _open(root) as store:
        assert advance_closing(
            store, limit=1, settle=lambda _member: pytest.fail("replayed decision")
        )
        assert store.load(_member()).decision_accepted is not None


def test_empty_inventory_and_lost_selection_are_distinct_from_a_fresh_allocation(tmp_path):
    from riverhog_client._prepared_primary_closing import advance_closing, freeze_inventory

    root = tmp_path.resolve() / "state"
    with _open(root, initialize=True) as store:
        assert freeze_inventory(store, ()).members == 0
        assert store.usage.inventory_selected and store.usage.inventory_bytes == 0
        assert advance_closing(store, limit=1, settle=_no_author)
    (root / "closing").unlink()
    with pytest.raises(PrimaryStateError, match="lost its closing control"):
        _open(root)


def test_corrupt_frozen_inventory_never_publishes_finish_ready(tmp_path):
    from riverhog_client._prepared_primary_closing import advance_closing, freeze_inventory

    root = tmp_path.resolve() / "state"
    with _open(root, initialize=True) as store:
        store.prepare_or_load(_member(), REQUEST, lambda _limit: _author())
        freeze_inventory(store, (_member(),))
        raw = (root / "inventory").read_bytes()
        value = json.loads(raw)
        value["prepared_sha256"] = "f" * 64
        (root / "inventory").write_bytes(
            json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        )
        assert (root / "inventory").stat().st_size == len(raw)
        with pytest.raises(PrimaryStateError, match="original preparation"):
            advance_closing(store, limit=1, settle=_no_author)
        assert not (root / "finish-ready").exists()


def test_finish_barrier_checks_the_complete_frozen_chain_after_settling_every_member(tmp_path):
    from riverhog_client._prepared_primary_closing import advance_closing, freeze_inventory

    root = tmp_path.resolve() / "state"
    with _open(root, initialize=True) as store:
        store.prepare_or_load(_member(), REQUEST, lambda _limit: _author())
        closing = freeze_inventory(store, (_member(),))
        changed = closing.model_copy(
            update={"inventory": closing.inventory.model_copy(update={"chain_sha256": "f" * 64})}
        )
        store._write(store._files, "closing", changed, expected=closing)

        def settle(member):
            store.accept_decision(store.load(member), b"exact decision")

        with pytest.raises(PrimaryStateError, match="frozen commitment"):
            advance_closing(store, limit=1, settle=settle)
        assert store.load(_member()).decision_accepted is not None
        assert not (root / "finish-ready").exists()


def _ready_inventory(store, inventory):
    from riverhog_client._prepared_primary_closing import advance_closing, freeze_inventory

    for member in inventory:
        record = store.prepare_or_load(
            member, REQUEST, lambda _limit, member=member: _author(member)
        )
        store.accept_decision(record, b"exact decision")
    freeze_inventory(store, inventory)
    assert advance_closing(store, limit=max(1, len(inventory)), settle=_no_author)


def test_terminal_custody_waits_for_exact_receipts_and_recovers_a_lost_cursor(tmp_path):
    from riverhog_client._prepared_primary_closing import advance_custody

    root = tmp_path.resolve() / "state"
    inventory = tuple(_member(number) for number in range(1, 4))
    with _open(root, initialize=True) as store:
        _ready_inventory(store, inventory)
        inventory_bytes = store.usage.inventory_bytes
        assert not advance_custody(store, limit=1, settle=lambda _member: None)
        assert json.loads((root / "custody-closing").read_bytes())["after_member"] == "0"
        assert not (root / "custody-settled").exists()

        def lost_cursor(member):
            record = store.load(member)
            store.accept_custody(record, _receipt(record.prepared))
            raise RuntimeError("lost custody cursor")

        with pytest.raises(RuntimeError, match="lost custody cursor"):
            advance_custody(store, limit=1, settle=lost_cursor)
        assert store.load(inventory[0]).custody is not None

    with _open(root) as store:
        calls = []

        def settle(member):
            calls.append(member.artifact_id)
            record = store.load(member)
            store.accept_custody(record, _receipt(record.prepared))

        assert not advance_custody(store, limit=1, settle=settle)
        assert calls == []
        assert not advance_custody(store, limit=1, settle=settle)
        assert advance_custody(store, limit=1, settle=settle)
        assert calls == [inventory[1].artifact_id, inventory[2].artifact_id]
        assert store.usage.pending_bytes == 0
        assert store.usage.inventory_bytes == inventory_bytes
        for member in inventory:
            assert store.load(member).compact
            assert store.prepare_or_load(member, REQUEST, _no_author).compact
        assert advance_custody(store, limit=1, settle=_no_author)


@pytest.mark.parametrize(
    "boundary",
    [
        "candidate-custody-closing",
        "custody-closing",
        "candidate-custody",
        "linked-custody",
        "custody",
        "unlinked-journal",
        "remove-journal",
        "candidate-reclaimed",
        "reclaimed",
        "candidate-usage",
        "usage",
        "candidate-custody-settled",
        "custody-settled",
    ],
)
def test_process_exit_during_terminal_custody_keeps_exact_receipts_and_accounting(
    tmp_path, boundary
):
    from riverhog_client._prepared_primary_closing import advance_custody

    root = tmp_path.resolve() / "state"
    with _open(root, initialize=True) as store:
        _ready_inventory(store, (_member(),))
    _crash(root, boundary, operation="custody")
    with _open(root) as store:
        calls = []

        def settle(member):
            calls.append(member.artifact_id)
            record = store.load(member)
            store.accept_custody(record, _receipt(record.prepared))

        assert advance_custody(store, limit=1, settle=settle)
        assert calls == (
            [_member().artifact_id]
            if boundary in ("candidate-custody-closing", "custody-closing", "candidate-custody")
            else []
        )
        assert store.usage.pending_bytes == 0
        assert store.load(_member()).compact
        assert advance_custody(store, limit=1, settle=_no_author)


def test_terminal_custody_does_not_invent_a_missing_decision_acknowledgement(tmp_path):
    from riverhog_client._prepared_primary_closing import advance_custody

    root = tmp_path.resolve() / "state"
    with _open(root, initialize=True) as store:
        _ready_inventory(store, (_member(),))
        with store._member(str(_member().artifact_id), create=False) as files:
            files.remove("decision-accepted")
        with pytest.raises(PrimaryStateError, match="exact decision acknowledgement"):
            advance_custody(store, limit=1, settle=_no_author)
        assert not (root / "custody-settled").exists()
        record = store.load(_member())
        with store.byte_reader(record, "decision") as stream:
            assert stream.read() == b"exact decision"


def test_terminal_custody_requires_its_decision_barrier_and_exact_frozen_chain(tmp_path):
    from riverhog_client._prepared_primary_closing import advance_custody

    root = tmp_path.resolve() / "state"
    with _open(root, initialize=True) as store:
        with pytest.raises(FileNotFoundError):
            advance_custody(store, limit=1, settle=_no_author)
        _ready_inventory(store, (_member(),))
        value = json.loads((root / "inventory").read_bytes())
        value["prepared_sha256"] = "f" * 64
        (root / "inventory").write_bytes(
            json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        )
        with pytest.raises(PrimaryStateError, match="original preparation"):
            advance_custody(store, limit=1, settle=_no_author)
        assert not (root / "custody-settled").exists()


@pytest.mark.parametrize("removed", ["finish-ready", "custody-closing"])
def test_terminal_release_authority_cannot_outlive_its_original_barrier(tmp_path, removed):
    from riverhog_client._prepared_primary_closing import advance_custody

    root = tmp_path.resolve() / "state"
    with _open(root, initialize=True) as store:
        _ready_inventory(store, ())

        def no_member(_member):
            pytest.fail("empty inventory requested an artifact")

        assert advance_custody(store, limit=1, settle=no_member)
    (root / removed).unlink()
    with pytest.raises(PrimaryStateError):
        _open(root)


def test_owner_inventory_orders_on_disk_with_fixed_scratch_admission_and_no_registry(tmp_path):
    from riverhog_client import ordered_primary_inventory
    from riverhog_client.producer import ProducerArtifactIdentity

    root = tmp_path.resolve() / "state"
    limits = replace(LIMITS, inventory_order_bytes=32 * 1024)
    members = tuple(
        ProducerArtifactIdentity(value.artifact_id, value.bytes, value.sha256)
        for value in (_member(3), _member(1), _member(2))
    )
    with _open(root, initialize=True, limits=limits) as store:
        assert list(ordered_primary_inventory(store, iter(members))) == [
            members[1],
            members[2],
            members[0],
        ]
        scratch = root / "inventory-ordering" / "selection.sqlite3"
        assert not scratch.exists()
        with pytest.raises(PrimaryStateError, match="duplicate artifact"):
            list(ordered_primary_inventory(store, (members[0], members[0])))
        assert not scratch.exists()
        # Exact named scratch from an interrupted sort is derived, never member authority.
        scratch.write_bytes(b"interrupted scratch")
        scratch.chmod(0o600)
        assert len(list(ordered_primary_inventory(store, members))) == 3
        assert not scratch.exists()
        assert store.usage.members == 0


def test_owner_inventory_scratch_capacity_failure_preserves_preparation_and_restarts(tmp_path):
    from riverhog_client import ordered_primary_inventory
    from riverhog_client.producer import ProducerArtifactIdentity

    root = tmp_path.resolve() / "state"
    limits = replace(LIMITS, inventory_order_bytes=8192)
    with _open(root, initialize=True, limits=limits) as store:
        original = store.prepare_or_load(_member(), REQUEST, lambda _limit: _author())
        before = store.usage

        def many_members():
            for number in range(1000):
                member = _member(number)
                yield ProducerArtifactIdentity(member.artifact_id, member.bytes, member.sha256)

        with pytest.raises(PrimaryStateCapacity, match="scratch admission"):
            list(ordered_primary_inventory(store, many_members()))
        assert store.load(_member()) == original and store.usage == before
        assert not (root / "inventory-ordering" / "selection.sqlite3").exists()


def test_closing_output_is_reserved_before_pending_and_total_admission_saturate(tmp_path):
    from riverhog_client._prepared_primary_closing import advance_closing, freeze_inventory

    size = len(REQUEST) + sum(
        len(raw) for raw in (b"original selection", b"original graph", JOURNAL, b"exact decision")
    )
    limits = replace(
        LIMITS,
        pending_bytes=size,
        total_bytes=LIMITS.root_reservation
        + LIMITS.member_reservation
        + LIMITS.control_bytes
        + 1
        + size,
    )
    with _open(tmp_path.resolve() / "state", initialize=True, limits=limits) as store:
        record = store.prepare_or_load(_member(), REQUEST, lambda _limit: _author())
        assert store.usage.inventory_reserved_bytes == limits.control_bytes + 1
        assert (
            limits.root_reservation
            + store.usage.control_bytes
            + store.usage.pending_bytes
            + store.usage.inventory_reserved_bytes
        ) == limits.total_bytes
        store.accept_decision(record, b"exact decision")
        closing = freeze_inventory(store, (_member(),))
        assert store.usage.inventory_reserved_bytes == 0
        assert (
            limits.root_reservation
            + store.usage.control_bytes
            + store.usage.pending_bytes
            + closing.inventory.bytes
        ) <= limits.total_bytes
        assert advance_closing(store, limit=1, settle=_no_author)


@pytest.mark.parametrize("mode", ["supplied", "lazy"])
def test_restored_observation_selection_keeps_its_original_mode_without_a_factory(
    tmp_path, native_observation, mode
):
    from riverhog_client._prepared_primary_publisher import (
        DeclaredPrimaryObservation,
        PrimaryPublisher,
        retained_primary_observation,
    )

    selection = native_observation
    calls = []
    if mode == "lazy":

        def observe():
            calls.append(True)
            return native_observation

        selection = DeclaredPrimaryObservation(
            "fixture-observer",
            {"source": "fixture"},
            native_observation.catalog.checkpoint_sha256(),
            observe,
        )
    root = tmp_path.resolve() / "state"
    api = _PublicationApi()
    with _open(root, initialize=True) as store:
        original = _publish(PrimaryPublisher(api, store), selection)
        restored = retained_primary_observation(selection)
        assert restored.inputs["mode"] == mode
    with _open(root) as store:
        replay = _publish(PrimaryPublisher(api, store), restored)
        assert replay.prepared == original.prepared
        assert calls == ([True] if mode == "lazy" else [])
    other = tmp_path.resolve() / "no-preparation"
    with _open(other, initialize=True) as store:
        with pytest.raises(PrimaryStateError, match="original preparation preimages"):
            _publish(PrimaryPublisher(api, store), restored)
        assert store.usage.members == 0


@pytest.mark.parametrize("change", ["unknown-field", "unknown-mode", "changed-observer"])
def test_restored_observation_declarations_reject_unrecognized_inputs_before_authorship(
    tmp_path, change
):
    from riverhog_client._prepared_primary_publisher import (
        PrimaryPublisher,
        RetainedPrimaryObservation,
        retained_primary_observation,
    )

    selection = dict(retained_primary_observation(None).inputs)
    if change == "unknown-field":
        selection["unknown"] = True
    elif change == "unknown-mode":
        selection["mode"] = "equivalent-replacement"
    else:
        selection["observer"] = "another-observer"
    with _open(tmp_path.resolve() / "state", initialize=True) as store:
        with pytest.raises(PrimaryStateError):
            _publish(
                PrimaryPublisher(_PublicationApi(), store), RetainedPrimaryObservation(selection)
            )
        assert store.usage.members == 0


@pytest.mark.parametrize("field", ["format", "kind"])
def test_control_readback_never_invents_missing_wire_version_or_kind(tmp_path, field):
    root = tmp_path.resolve() / "state"
    with _open(root, initialize=True):
        pass
    value = json.loads((root / "usage").read_bytes())
    value.pop(field)
    (root / "usage").write_bytes(json.dumps(value, sort_keys=True, separators=(",", ":")).encode())
    with pytest.raises(PrimaryStateError, match="exact current encoding"):
        _open(root)


def test_reserved_custody_commitment_progresses_when_full_receipt_exceeds_control_admission(
    tmp_path,
):
    from riverhog_canonical_json import canonical_json_sha256

    member = _member()
    with _open(tmp_path.resolve() / "shape", initialize=True) as store:
        template = store.prepare_or_load(member, REQUEST, lambda _limit: _author())
        control_bytes = len(primary_control_bytes(template.prepared))
    pending = len(REQUEST) + sum(
        len(value)
        for value in (b"original selection", b"original graph", JOURNAL, b"exact decision")
    )
    limits = replace(
        LIMITS,
        control_bytes=control_bytes,
        pending_bytes=pending,
        preparation_bytes=pending,
        total_bytes=20 * control_bytes + 1 + pending,
    )
    root = tmp_path.resolve() / "saturated"
    with _open(root, initialize=True, limits=limits) as store:
        original = store.prepare_or_load(member, REQUEST, lambda _limit: _author())
        assert (
            limits.root_reservation
            + store.usage.control_bytes
            + store.usage.inventory_reserved_bytes
            + store.usage.pending_bytes
            == limits.total_bytes
        )
        with pytest.raises(PrimaryStateCapacity):
            store.prepare_or_load(_member(2), REQUEST, _no_author)
        small_receipt = _receipt(original.prepared)
        value = small_receipt.model_dump(mode="json")
        value["archive_object_count"] = "1" + "0" * 2000
        value["provenance_object_count"] = "1" + "0" * 2000
        value["receipt_sha256"] = canonical_json_sha256(
            {key: item for key, item in value.items() if key != "receipt_sha256"}
        )
        receipt = CollectionUploadArtifactCustodyReceiptDocument.model_validate(value)
        assert len(primary_control_bytes(receipt)) > limits.control_bytes
        retained = store.accept_custody(original, receipt)
        assert not retained.compact and store.usage.pending_bytes == pending
        assert retained.custody.receipt_sha256 == receipt.receipt_sha256
        assert len(primary_control_bytes(retained.custody)) <= limits.control_bytes
        with pytest.raises(PrimaryStateError, match="immutable control conflicts"):
            store.accept_custody(retained, small_receipt)
        retained = store.accept_decision(retained, b"exact decision")
        assert retained.compact and store.reclaim(member)
        assert store.usage.pending_bytes == 0
    with _open(root, limits=limits) as store:
        loaded = store.load(member)
        assert loaded.prepared == original.prepared
        assert loaded.custody.receipt_sha256 == receipt.receipt_sha256
        assert store.accept_custody(loaded, receipt) == loaded
        assert store.reclaim(member)


@pytest.mark.parametrize("role", ["request", "observation", "graph", "decision"])
def test_original_manifest_seal_rejects_metadata_corruption_before_later_anchors(tmp_path, role):
    from riverhog_canonical_json import canonical_json_bytes

    root = tmp_path.resolve() / "state"
    with _open(root, initialize=True) as store:
        original = store.prepare_or_load(_member(), REQUEST, lambda _limit: _author())
        with store._member(str(_member().artifact_id), create=False) as files:
            path = files.root / "prepared"
            assert not files.exists("custody") and not files.exists("decision-accepted")
    value = json.loads(path.read_bytes())
    value[role]["sha256"] = "f" * 64
    path.write_bytes(canonical_json_bytes(value))
    with _open(root) as store:
        with pytest.raises(ValueError, match="original manifest integrity"):
            store.load_controls(_member())
        with pytest.raises(ValueError, match="original manifest integrity"):
            store.accept_custody(original, _receipt(original.prepared))
        assert store.usage.pending_bytes > 0


def test_receipt_read_cannot_bless_a_missing_original_manifest_seal(tmp_path):
    from riverhog_canonical_json import canonical_json_bytes

    root = tmp_path.resolve() / "state"
    with _open(root, initialize=True) as store:
        store.prepare_or_load(_member(), REQUEST, lambda _limit: _author())
        with store._member(str(_member().artifact_id), create=False) as files:
            path = files.root / "prepared"
    value = json.loads(path.read_bytes())
    del value["manifest_sha256"]
    path.write_bytes(canonical_json_bytes(value))
    with _open(root) as store:
        with pytest.raises(ValueError, match="manifest_sha256"):
            store.load_controls(_member())
        assert "manifest_sha256" not in json.loads(path.read_bytes())


@pytest.mark.parametrize("role", ["request", "observation", "graph", "journal", "decision"])
@pytest.mark.parametrize("damage", ["missing", "truncated", "changed"])
def test_receipt_controls_survive_unreplayable_preimages_without_inventing_decision(
    tmp_path, role, damage
):
    from riverhog_client._prepared_primary_publisher import PrimaryPublisher

    root = tmp_path.resolve() / "state"
    with _open(root, initialize=True) as store:
        original = store.prepare_or_load(_member(), REQUEST, lambda _limit: _author())
        pending = store.usage.pending_bytes
        with store._member(str(_member().artifact_id), create=False) as files:
            path = files.root / role
            content = path.read_bytes()
    if damage == "missing":
        path.unlink()
    else:
        path.write_bytes(content[:1] if damage == "truncated" else b"x" * len(content))
    with _open(root) as store:
        assert store.load_controls(_member()) == original
        retained = store.accept_custody(original, _receipt(original.prepared))
        assert retained.custody is not None and retained.decision_accepted is None
        assert not retained.compact and not store.reclaim(_member())
        assert store.usage.pending_bytes == pending
        api = _PublicationApi()
        with pytest.raises(PrimaryStateError):
            PrimaryPublisher(api, store).replay(retained)
        assert not api.journals and not api.decision_calls
        assert store.load_controls(_member()) == retained
    with _open(root) as store:
        assert store.load_controls(_member()) == retained
        assert store.usage.pending_bytes == pending
        assert not store.reclaim(_member())


def test_receipt_checks_stale_prepared_handle_against_original_sealed_manifest(tmp_path):
    from riverhog_client._prepared_primary_documents import PreparedPrimary

    with _open(tmp_path.resolve() / "state", initialize=True) as store:
        original = store.prepare_or_load(_member(), REQUEST, lambda _limit: _author())
        value = original.prepared.model_dump(mode="json", exclude={"manifest_sha256"})
        value["request"]["sha256"] = "e" * 64
        stale = replace(original, prepared=PreparedPrimary.seal(**value))
        with pytest.raises(PrimaryStateError, match="matching manifest"):
            store.accept_custody(stale, _receipt(original.prepared))
        assert store.load_controls(_member()).custody is None


@contextmanager
def _receipt_work(monkeypatch):
    """Observe real local primitives; the store fixture does not execute Native authorship."""
    counts = dict(read_bytes=0, pending_read_bytes=0, hash_bytes=0, opens=0, fsyncs=0, unlinks=0)
    roles = {}
    open_file, fdopen, sha256 = filesystem.PrimaryFiles._open, os.fdopen, hashlib.sha256
    fsync, unlink = os.fsync, os.unlink
    pending = {"request", "observation", "graph", "journal", "decision"}

    def opened(files, name):
        descriptor = open_file(files, name)
        roles[descriptor] = name
        counts["opens"] += 1
        return descriptor

    class Reader:
        def __init__(self, stream, role):
            self.stream, self.role = stream, role

        def __enter__(self):
            self.stream.__enter__()
            return self

        def __exit__(self, *args):
            return self.stream.__exit__(*args)

        def __getattr__(self, name):
            return getattr(self.stream, name)

        def counted(self, value):
            counts["read_bytes"] += len(value)
            if self.role in pending:
                counts["pending_read_bytes"] += len(value)
            return value

        def read(self, *args):
            return self.counted(self.stream.read(*args))

        def readline(self, *args):
            return self.counted(self.stream.readline(*args))

    class Digest:
        def __init__(self, digest):
            self.digest = digest

        def __getattr__(self, name):
            return getattr(self.digest, name)

        def update(self, value):
            counts["hash_bytes"] += len(value)
            return self.digest.update(value)

    def hash_bytes(value=b"", *args, **kwargs):
        counts["hash_bytes"] += len(value)
        return Digest(sha256(value, *args, **kwargs))

    def synced(descriptor):
        counts["fsyncs"] += 1
        return fsync(descriptor)

    def removed(*args, **kwargs):
        result = unlink(*args, **kwargs)
        counts["unlinks"] += 1
        return result

    def no_scan(*_args, **_kwargs):
        pytest.fail("selected-member receipt work enumerated unrelated members")

    with monkeypatch.context() as patch:
        patch.setattr(filesystem.PrimaryFiles, "_open", opened)
        patch.setattr(
            os, "fdopen", lambda fd, *a, **kw: Reader(fdopen(fd, *a, **kw), roles.get(fd))
        )
        patch.setattr(hashlib, "sha256", hash_bytes)
        patch.setattr(os, "fsync", synced)
        patch.setattr(os, "unlink", removed)
        patch.setattr(os, "scandir", no_scan)
        patch.setattr(Path, "iterdir", no_scan)
        started = time.perf_counter()
        yield counts
        counts["seconds"] = time.perf_counter() - started


@pytest.mark.parametrize("large_role", ["baseline", "request", "observation", "graph", "journal"])
def test_cold_receipt_all_stages_read_controls_only_as_independent_preimages_grow(
    tmp_path, monkeypatch, large_role
):
    member = _member()
    authored = _author()
    request = REQUEST
    if large_role == "request":
        request = b"r" * (1024 * 1024)
    elif large_role in ("observation", "graph"):
        authored = replace(authored, **{large_role: b"p" * (1024 * 1024)})
    elif large_role == "journal":
        journal = b"j" * (1024 * 1024)
        primary = authored.primary.model_dump(mode="json")
        primary["journal"].update(
            prefix_bytes=str(len(journal)), prefix_sha256=hashlib.sha256(journal).hexdigest()
        )
        authored = replace(
            authored,
            journal=journal,
            primary=CollectionArtifactProvenanceBindingDocument.model_validate(primary),
        )
    limits = replace(
        LIMITS, pending_bytes=4 * 1024**2, total_bytes=8 * 1024**2, preparation_bytes=2 * 1024**2
    )
    root = tmp_path.resolve() / "state"
    with _open(root, initialize=True, limits=limits) as store:
        original = store.prepare_or_load(member, request, lambda _limit: authored)
    receipt = _receipt(original.prepared)
    samples = {}
    with _receipt_work(monkeypatch) as cold:
        store = _open(root, limits=limits)
        assert store.load_controls(member) == original
    samples["cold_control_load"] = dict(cold)
    try:
        with _receipt_work(monkeypatch) as accept:
            retained = store.accept_custody(original, receipt)
            assert retained.custody is not None and retained.decision_accepted is None
        samples["receipt_admission_and_return"] = dict(accept)
        with _receipt_work(monkeypatch) as waiting:
            assert not store.reclaim(member)
        samples["unacknowledged_reclaim"] = dict(waiting)
        # The real decision acknowledgement is a separate, byte-consuming phase.
        store.accept_decision(retained, authored.decision)
        with _receipt_work(monkeypatch) as reclaim:
            assert store.reclaim(member)
            assert store.reclaim(member)
            assert store.usage.pending_bytes == 0
        samples["acknowledged_reclaim_and_retry"] = dict(reclaim)
        for sample in samples.values():
            assert sample["pending_read_bytes"] == 0
            assert (
                sample["hash_bytes"]
                <= 8 * (sample["read_bytes"] + len(primary_control_bytes(receipt))) + 512
            )
            assert sample["fsyncs"] > 0
        print("prepared_receipt_work", json.dumps({"large_role": large_role, "samples": samples}))
    finally:
        store.close()


def test_interrupted_preparation_byte_recovery_is_separate_from_receipt_work(tmp_path, monkeypatch):
    root = tmp_path.resolve() / "state"
    with _open(root, initialize=True):
        pass
    _crash(root, "prepared")
    with _receipt_work(monkeypatch) as recovery:
        store = _open(root)
    try:
        assert recovery["pending_read_bytes"] > 0
        with _receipt_work(monkeypatch) as receipt_phase:
            original = store.load_controls(_member())
            store.accept_custody(original, _receipt(original.prepared))
        assert receipt_phase["pending_read_bytes"] == 0
        print(
            "prepared_transition_work", json.dumps({"recovery": recovery, "receipt": receipt_phase})
        )
    finally:
        store.close()


def test_terminal_receipt_callback_and_reclamation_never_read_redundant_preimages(
    tmp_path, monkeypatch
):
    from riverhog_client._prepared_primary_closing import advance_custody

    root = tmp_path.resolve() / "state"
    with _open(root, initialize=True) as store:
        _ready_inventory(store, (_member(),))
        original = store.load(_member())
        with store._member(str(_member().artifact_id), create=False) as files:
            for file in original.prepared.byte_files:
                (files.root / file.name).unlink()
    with _receipt_work(monkeypatch) as terminal:
        with _open(root) as store:

            def settle(member):
                record = store.load_controls(member)
                store.accept_custody(record, _receipt(record.prepared))

            assert advance_custody(store, limit=1, settle=settle)
            assert store.usage.pending_bytes == 0
            assert store.custody_settled
            assert advance_custody(store, limit=1, settle=_no_author)
    assert terminal["pending_read_bytes"] == 0
    assert terminal["fsyncs"] > 0
    print("prepared_terminal_receipt_work", json.dumps(terminal))


@pytest.mark.parametrize("role", ["request", "observation", "graph", "journal", "decision"])
def test_bounded_reclaim_retains_original_charge_across_cold_pending_steps(
    tmp_path, monkeypatch, role
):
    from riverhog_client import PrimaryStateBusy

    root = tmp_path.resolve() / "state"
    with _open(root, initialize=True) as store:
        record = store.prepare_or_load(_member(), REQUEST, lambda _limit: _author())
        record = store.accept_decision(record, b"exact decision")
        record = store.accept_custody(record, _receipt(record.prepared))
        before = store.usage
    remove_step = filesystem.PrimaryFiles.remove_step
    paused = True
    calls = 0

    def pending(files, name):
        nonlocal calls
        if name == role and paused:
            calls += 1
            return False
        return remove_step(files, name)

    monkeypatch.setattr(filesystem.PrimaryFiles, "remove_step", pending)
    with _open(root) as store:
        assert not store.reclaim(_member())
        assert calls == 1
        transition = (root / "transition").read_bytes()
        for observe in [
            lambda: store.usage,
            lambda: store.session_bound,
            lambda: store.publication_started,
            lambda: store.custody_settled,
            lambda: store.load_controls(_member()),
        ]:
            previous = calls
            observe()
            assert calls == previous + 1
            assert store._usage == before
            assert (root / "transition").read_bytes() == transition
        with store._member(str(_member().artifact_id), create=False) as files:
            assert not files.exists("reclaimed")
            assert files.read_control(role, byte_limit=LIMITS.control_bytes)
        with pytest.raises(PrimaryStateBusy, match="bounded cleanup"):
            store.prepare_or_load(_member(2), REQUEST, _no_author)
    previous = calls
    with _open(root) as store:
        assert calls == previous + 1
        assert store._usage == before
        paused = False
        assert store.reclaim(_member())
        after = store.usage
        assert after.pending_bytes == 0 and after.sequence == before.sequence + 1
        assert after.members == before.members
        assert after.control_bytes == before.control_bytes
        assert not (root / "transition").exists()
        assert store.reclaim(_member()) and store.usage == after
    with _open(root) as store:
        assert store.reclaim(_member()) and store.usage == after


@pytest.mark.parametrize("role", ["request", "observation", "graph", "journal", "decision"])
def test_terminal_custody_cursor_waits_for_all_bounded_role_deletions(tmp_path, monkeypatch, role):
    from riverhog_client._prepared_primary_closing import advance_custody

    root = tmp_path.resolve() / "state"
    remove_step = filesystem.PrimaryFiles.remove_step
    paused = True
    calls = []

    def pending(files, name):
        if name == role and paused:
            return False
        return remove_step(files, name)

    with _open(root, initialize=True) as store:
        _ready_inventory(store, (_member(),))
        before = store.usage
        monkeypatch.setattr(filesystem.PrimaryFiles, "remove_step", pending)

        def settle(member):
            calls.append(member)
            record = store.load_controls(member)
            store.accept_custody(record, _receipt(record.prepared))

        assert not advance_custody(store, limit=1, settle=settle)
        cursor = (root / "custody-closing").read_bytes()
        assert json.loads(cursor)["after_member"] == "0"
        assert len(calls) == 1 and store._usage == before
    with _open(root) as store:
        for _ in range(3):
            assert not advance_custody(store, limit=1, settle=settle)
            assert (root / "custody-closing").read_bytes() == cursor
            assert store._usage == before
            assert not (root / "custody-settled").exists()
            assert len(calls) == 1
        paused = False
        assert advance_custody(store, limit=1, settle=settle)
        assert len(calls) == 1
        assert store.custody_settled
        assert store.usage.pending_bytes == 0
        assert json.loads((root / "custody-closing").read_bytes())["after_member"] == "1"


def test_interrupted_preparation_keeps_its_transition_until_bounded_rollback_finishes(
    tmp_path, monkeypatch
):
    from riverhog_client import PrimaryStateBusy

    root = tmp_path.resolve() / "state"
    with _open(root, initialize=True):
        pass
    _crash(root, "graph")
    remove_step = filesystem.PrimaryFiles.remove_step
    paused = True

    def pending(files, name):
        if name == "graph" and paused:
            return False
        return remove_step(files, name)

    monkeypatch.setattr(filesystem.PrimaryFiles, "remove_step", pending)
    with _open(root) as store:
        assert store.usage.members == store.usage.pending_bytes == 0
        assert (root / "transition").exists()
        with pytest.raises(PrimaryStateBusy, match="bounded cleanup"):
            store.prepare_or_load(_member(), REQUEST, _no_author)
        paused = False
        record = store.prepare_or_load(_member(), REQUEST, lambda _limit: _author())
        assert not record.compact
        assert store.usage.members == 1
        assert not (root / "transition").exists()


def test_unselected_inventory_rebuild_waits_for_bounded_deletion(tmp_path, monkeypatch):
    from riverhog_client import PrimaryStateBusy
    from riverhog_client._prepared_primary_closing import freeze_inventory

    root = tmp_path.resolve() / "state"
    with _open(root, initialize=True) as store:
        store.prepare_or_load(_member(), REQUEST, lambda _limit: _author())
    _crash(root, "inventory", operation="freeze")
    remove_step = filesystem.PrimaryFiles.remove_step
    paused = True

    def pending(files, name):
        if name == "inventory" and paused:
            return False
        return remove_step(files, name)

    monkeypatch.setattr(filesystem.PrimaryFiles, "remove_step", pending)
    with _open(root) as store:
        original_intent = (root / "closing-intent").read_bytes()
        assert not store.usage.inventory_selected
        with pytest.raises(PrimaryStateBusy, match="bounded cleanup"):
            freeze_inventory(store, (_member(),))
        assert (root / "closing-intent").read_bytes() == original_intent
        paused = False
        assert freeze_inventory(store, (_member(),)).members == 1
        assert store.usage.inventory_selected


@pytest.fixture
def sql_core_storage():
    from riverhog_client import _prepared_primary_sqlite as sql

    from tests.support.prepared_primary import SqlitePrimaryTestGuard

    def opened(path, *, allocation_descriptor, initialize):
        if initialize:
            path.touch(exist_ok=False)
        return sql.TransactionalPrimaryFiles(
            SqlitePrimaryTestGuard(path),
            allocation_descriptor=allocation_descriptor,
            limits=sql.SqlitePrimaryLimits(16 * 1024**2, 32 * 1024**2, 128 * 1024),
            initialize=initialize,
        )

    return opened


def test_shared_sql_core_replays_original_and_retains_charge_through_cold_reclaim(
    tmp_path, sql_core_storage
):
    import sqlite3

    root = tmp_path.resolve() / "state.sqlite3"
    journal = b"original journal" * 1000
    with _open(root, storage_factory=sql_core_storage, initialize=True) as store:
        original = store.prepare_or_load(
            _member(), REQUEST, lambda _limit: _author(journal=journal)
        )
        before = store.usage
    with _open(root, storage_factory=sql_core_storage) as store:
        assert store.prepare_or_load(_member(), REQUEST, _no_author) == original
        with store.byte_reader(original, "journal") as stream:
            assert stream.read() == journal
        record = store.accept_decision(original, b"exact decision")
        store.accept_custody(record, _receipt(record.prepared))
        assert not store.reclaim(_member())
        assert store._usage == before

    with _open(root, storage_factory=sql_core_storage) as store:
        assert store._usage == before
        statements = []
        store._files._connection.set_trace_callback(statements.append)

        def controls_only(action, table, column, *_rest):
            if action == sqlite3.SQLITE_READ and table == "primary_chunks" and column == "body":
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        store._files._connection.set_authorizer(controls_only)
        for _ in range(64):
            statements.clear()
            complete = store.reclaim(_member())
            deletes = [s for s in statements if s.startswith("DELETE FROM primary_chunks")]
            assert len(deletes) <= 1
            if complete:
                break
            assert store._usage == before
        else:
            pytest.fail("bounded fixture reclamation did not finish")
        after = store.usage
        assert after.pending_bytes == 0 and after.sequence == before.sequence + 1
        assert after.control_bytes == before.control_bytes
        assert store.reclaim(_member()) and store.usage == after
    with _open(root, storage_factory=sql_core_storage) as store:
        assert store.load_controls(_member()).compact
        assert store.usage == after


def test_shared_sql_core_terminal_cursor_waits_for_every_original_role(tmp_path, sql_core_storage):
    from riverhog_client._prepared_primary_closing import advance_custody

    root = tmp_path.resolve() / "state.sqlite3"
    calls = []
    with _open(root, storage_factory=sql_core_storage, initialize=True) as store:
        _ready_inventory(store, (_member(),))
        before = store.usage

        def settle(member):
            calls.append(member)
            record = store.load_controls(member)
            store.accept_custody(record, _receipt(record.prepared))

        assert not advance_custody(store, limit=1, settle=settle)
        cursor = store._files.read_control("custody-closing", byte_limit=LIMITS.control_bytes)
        assert json.loads(cursor)["after_member"] == "0"
    with _open(root, storage_factory=sql_core_storage) as store:
        for _ in range(64):
            complete = advance_custody(store, limit=1, settle=settle)
            if complete:
                break
            assert store._usage == before
            assert (
                store._files.read_control("custody-closing", byte_limit=LIMITS.control_bytes)
                == cursor
            )
        else:
            pytest.fail("bounded terminal fixture did not finish")
        assert calls == [_member()]
        assert store.custody_settled and store.usage.pending_bytes == 0


def test_shared_sql_core_cold_candidate_rollback_never_adopts_an_unsealed_primary(
    tmp_path, monkeypatch, sql_core_storage
):
    from riverhog_client import _prepared_primary_sqlite as sql

    root = tmp_path.resolve() / "state.sqlite3"
    select = sql.TransactionalPrimaryFiles._select

    def interrupted(files, generation, result):
        if result.name == "journal":
            raise RuntimeError("before original journal selection")
        return select(files, generation, result)

    with _open(root, storage_factory=sql_core_storage, initialize=True) as store:
        with monkeypatch.context() as patch:
            patch.setattr(sql.TransactionalPrimaryFiles, "_select", interrupted)
            with pytest.raises(RuntimeError, match="before original"):
                store.prepare_or_load(
                    _member(), REQUEST, lambda _limit: _author(journal=b"journal" * 2000)
                )
    with _open(root, storage_factory=sql_core_storage) as store:
        for _ in range(64):
            assert store._usage.pending_bytes == store._usage.members == 0
            if store._recover(require_complete=False):
                break
        else:
            pytest.fail("bounded candidate fixture rollback did not finish")
        assert store.load(_member()) is None
        record = store.prepare_or_load(_member(), REQUEST, lambda _limit: _author())
        assert not record.compact and store.usage.members == 1


def test_root_candidate_reconciliation_finishes_once_per_cold_owner(tmp_path, monkeypatch):
    root = tmp_path.resolve() / "state"
    original = filesystem.PrimaryFiles.discard_candidates
    calls = []

    def observed(files, names):
        if files._owner_root is files:
            calls.append(names)
        return original(files, names)

    monkeypatch.setattr(filesystem.PrimaryFiles, "discard_candidates", observed)
    with _open(root, initialize=True) as store:
        for _ in range(10):
            assert store.usage.members == 0
            assert store.session_bound and store.publication_started
        assert len(calls) == 1
    with _open(root) as store:
        for _ in range(10):
            assert store.usage.members == 0
        assert len(calls) == 2


def test_inventory_initialization_precedes_the_posix_byte_candidate(tmp_path):
    from riverhog_client._prepared_primary_closing import freeze_inventory

    root = tmp_path.resolve() / "state"
    members = [_member(1), _member(2)]
    with _open(root, initialize=True) as store:
        for member in members:
            store.prepare_or_load(member, REQUEST, lambda _limit, member=member: _author(member))
        seen = []

        def inventory():
            for member in members:
                seen.append((root / "candidate-inventory").exists())
                yield member

        assert freeze_inventory(store, inventory()).members == 2
        assert seen == [False, True]


def test_inventory_initialization_precedes_the_sql_singleton_byte_candidate(
    tmp_path, sql_core_storage
):
    from riverhog_client._prepared_primary_closing import freeze_inventory

    root = tmp_path.resolve() / "state.sqlite3"
    members = [_member(1), _member(2)]
    with _open(root, storage_factory=sql_core_storage, initialize=True) as store:
        for member in members:
            store.prepare_or_load(member, REQUEST, lambda _limit, member=member: _author(member))
        seen = []

        def inventory():
            for member in members:
                operation = store._files._one(
                    "SELECT o.name FROM primary_operation p "
                    "JOIN primary_objects o ON o.generation=p.generation WHERE p.id=1"
                )
                if not seen:
                    store._files._require_idle()
                assert not store._files._connection.in_transaction
                seen.append(operation)
                yield member

        assert freeze_inventory(store, inventory()).members == 2
        assert seen == [None, ("inventory",)]


def test_shared_sql_core_orders_lazy_inventory_before_publishing_and_settling(
    tmp_path, sql_core_storage
):
    from riverhog_client import ordered_primary_inventory
    from riverhog_client._prepared_primary_closing import (
        advance_closing,
        advance_custody,
        freeze_inventory,
    )
    from riverhog_client.producer import ProducerArtifactIdentity, _member_identity

    root = tmp_path.resolve() / "state.sqlite3"
    members = [_member(2), _member(1)]
    limits = replace(LIMITS, inventory_order_bytes=128 * 1024)
    with _open(root, storage_factory=sql_core_storage, initialize=True, limits=limits) as store:
        for member in members:
            store.prepare_or_load(member, REQUEST, lambda _limit, member=member: _author(member))
        supplied = (
            ProducerArtifactIdentity(member.artifact_id, member.bytes, member.sha256)
            for member in members
        )
        closing = freeze_inventory(
            store, map(_member_identity, ordered_primary_inventory(store, supplied))
        )
        assert closing.members == 2 and store.usage.inventory_selected
        assert store._files._one("SELECT phase FROM primary_ordering") == ("ready",)

        def decision(member):
            store.accept_decision(store.load(member), b"exact decision")

        assert advance_closing(store, limit=2, settle=decision)
        receipts = []

        def custody(member):
            receipts.append(member)
            record = store.load_controls(member)
            store.accept_custody(record, _receipt(record.prepared))

        for _ in range(64):
            if advance_custody(store, limit=2, settle=custody):
                break
        else:
            pytest.fail("bounded ordered custody fixture did not finish")
        assert receipts == members[::-1]
        assert store.custody_settled and store.usage.pending_bytes == 0


@pytest.mark.parametrize("backend", ["posix", "sqlite"])
def test_retained_ordering_stays_charged_until_explicit_cleanup_after_budget_reduction(
    tmp_path, sql_core_storage, backend
):
    root = tmp_path.resolve() / ("state" if backend == "posix" else "state.sqlite3")
    factory = None if backend == "posix" else sql_core_storage
    original_limits = replace(LIMITS, inventory_order_bytes=128 * 1024)
    with _open(root, initialize=True, limits=original_limits, storage_factory=factory) as store:
        original = store.prepare_or_load(_member(), REQUEST, lambda _grant: _author())
        before = store.usage
        if backend == "sqlite":
            assert list(store._files.ordered_rows([("a", 1, "a" * 64)], byte_limit=128 * 1024)) == [
                ("a", 1, "a" * 64)
            ]
        else:
            with store._files.child("inventory-ordering", initialize=True) as scratch:
                scratch.put("selection.sqlite3", [b"x" * (128 * 1024)], byte_limit=128 * 1024)
        assert store._files.ordering_reserved_bytes() == 128 * 1024

    size = sum(file.bytes for file in original.prepared.byte_files)
    reduced = replace(
        LIMITS,
        inventory_order_bytes=0,
        total_bytes=(
            LIMITS.root_reservation
            + before.control_bytes
            + before.pending_bytes
            + before.inventory_reserved_bytes
            + LIMITS.member_reservation
            + LIMITS.control_bytes
            + 1
            + size
            + 32
        ),
    )
    with _open(root, limits=reduced, storage_factory=factory) as store:
        for _ in range(3):
            assert store.usage == before
            assert store.session_bound
            assert store._files.ordering_reserved_bytes() == 128 * 1024
        with pytest.raises(PrimaryStateCapacity, match="admission exhausted"):
            store.prepare_or_load(_member(2), REQUEST, _no_author)
        assert store.load(_member()) == original

        # Reduced admission must not invalidate original compact-control authority.
        acknowledged = store.accept_decision(original, b"exact decision")
        store.accept_custody(acknowledged, _receipt(original.prepared))
        for _ in range(64):
            if store.reclaim(_member()):
                break
        else:
            pytest.fail("original receipt-backed reclaim did not finish")
        assert store._files.ordering_reserved_bytes() == 128 * 1024
        for _ in range(8):
            if store.cleanup_ordering_step():
                break
            assert store._files.ordering_reserved_bytes() == 128 * 1024
        else:
            pytest.fail("bounded scratch cleanup did not finish")
        assert store._files.ordering_reserved_bytes() == 0
        second = store.prepare_or_load(_member(2), REQUEST, lambda _grant: _author(_member(2)))
        assert second.prepared.member == _member(2)
        assert store.usage.members == 2


@pytest.mark.parametrize("backend", ["posix", "sqlite"])
def test_larger_ordering_grant_cannot_borrow_existing_member_reservations(
    tmp_path, sql_core_storage, backend
):
    from riverhog_client import ordered_primary_inventory

    root = tmp_path.resolve() / ("state" if backend == "posix" else "state.sqlite3")
    factory = None if backend == "posix" else sql_core_storage
    with _open(root, initialize=True, storage_factory=factory) as store:
        original = store.prepare_or_load(_member(), REQUEST, lambda _grant: _author())
        before = store.usage
    increased = replace(
        LIMITS,
        inventory_order_bytes=128 * 1024,
        total_bytes=(
            14 * LIMITS.control_bytes
            + 128 * 1024
            + before.control_bytes
            + before.pending_bytes
            + before.inventory_reserved_bytes
            - 1
        ),
    )

    def forbidden_input():
        pytest.fail("unadmitted ordering consumed owner input")
        yield

    with _open(root, limits=increased, storage_factory=factory) as store:
        with pytest.raises(PrimaryStateCapacity, match="ordering exceeds allocation"):
            list(ordered_primary_inventory(store, forbidden_input()))
        assert store._files.ordering_reserved_bytes() == 0
        assert store.usage == before and store.load(_member()) == original


def _deny_primary_chunk_bodies(factory):
    import sqlite3

    def opened(path, *, allocation_descriptor, initialize):
        files = factory(path, allocation_descriptor=allocation_descriptor, initialize=initialize)

        def control_reads(action, table, column, *_rest):
            if action == sqlite3.SQLITE_READ and table == "primary_chunks" and column == "body":
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        files._connection.set_authorizer(control_reads)
        return files

    return opened


def test_selected_prepare_control_recovery_defers_preimage_verification_to_execution(
    tmp_path, monkeypatch, sql_core_storage
):
    import sqlite3

    root = tmp_path.resolve() / "state.sqlite3"
    with _open(root, initialize=True, storage_factory=sql_core_storage) as store:

        def interrupted(*_args):
            raise RuntimeError("after original manifest selection")

        with monkeypatch.context() as patch:
            patch.setattr(store, "_finish_transition", interrupted)
            with pytest.raises(RuntimeError, match="original manifest"):
                store.prepare_or_load(_member(), REQUEST, lambda _grant: _author())

        def control_reads(action, table, column, *_rest):
            if action == sqlite3.SQLITE_READ and table == "primary_chunks" and column == "body":
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        store._files._connection.set_authorizer(control_reads)
        assert store.usage.members == 0
        assert store._files.exists("transition")
        original = store.load_controls(_member())
        assert original is not None
        assert store.session_for(CREATE) == store.session
        receipt = store.accept_custody(original, _receipt(original.prepared))
        assert receipt.custody is not None and not receipt.compact
        assert not store.reclaim(_member())
        assert store.usage.members == 0
        with pytest.raises(sqlite3.DatabaseError, match="prohibited"):
            store.load(_member())
        store._files._connection.set_authorizer(None)
        statements = []
        store._files._connection.set_trace_callback(statements.append)
        assert store.recover_step()
        assert any("SELECT body FROM primary_chunks" in q for q in statements)
        assert store.usage.members == 1
        assert not store._files.exists("transition")
        assert store.load(_member()).prepared == original.prepared


def test_bound_session_and_receipt_progress_during_unrelated_pending_candidate_cleanup(
    tmp_path, monkeypatch, sql_core_storage
):
    from riverhog_client import _prepared_primary_sqlite as sql

    root = tmp_path.resolve() / "state.sqlite3"
    with _open(root, initialize=True, storage_factory=sql_core_storage) as store:
        original = store.prepare_or_load(_member(), REQUEST, lambda _grant: _author())
        original = store.accept_decision(original, b"exact decision")
        before = store.usage
        select = sql.TransactionalPrimaryFiles._select

        def interrupted(files, generation, result):
            if result.name == "journal":
                raise RuntimeError("before unrelated journal selection")
            return select(files, generation, result)

        with monkeypatch.context() as patch:
            patch.setattr(sql.TransactionalPrimaryFiles, "_select", interrupted)
            with pytest.raises(RuntimeError, match="unrelated journal"):
                store.prepare_or_load(
                    _member(2), REQUEST, lambda _grant: _author(_member(2), journal=b"j" * 131072)
                )

    with _open(root, storage_factory=_deny_primary_chunk_bodies(sql_core_storage)) as store:
        operation = store._files._one("SELECT generation,cursor FROM primary_operation")
        assert operation is not None
        assert store.session_for(CREATE) == store.session
        accepted = store.accept_custody(original, _receipt(original.prepared))
        assert accepted.compact and accepted.prepared == original.prepared
        after = store._files._one("SELECT generation,cursor FROM primary_operation")
        assert after is not None and after[0] == operation[0]
        assert 0 <= after[1] - operation[1] <= 3
        assert store._usage == before
        statements = []
        store._files._connection.set_trace_callback(statements.append)
        for _ in range(128):
            statements.clear()
            complete = store.recover_step()
            assert sum(s.startswith("DELETE FROM primary_chunks") for s in statements) <= 1
            assert not store._files._connection.in_transaction
            if complete:
                break
            assert store._usage == before
        else:
            pytest.fail("bounded execution recovery did not finish")
        assert store._usage == before
        assert store.load_controls(_member()).compact
        assert store.load_controls(_member(2)) is None


def test_explicit_recovery_steps_finish_interrupted_ordering_without_status_draining_it(
    tmp_path, sql_core_storage
):
    root = tmp_path.resolve() / "state.sqlite3"
    rows = [(f"{number:064x}", number, "a" * 64) for number in range(17)]
    with _open(root, initialize=True, storage_factory=sql_core_storage) as store:
        original = store.prepare_or_load(_member(), REQUEST, lambda _grant: _author())

        def interrupted():
            yield from rows
            raise RuntimeError("interrupted derived input")

        with pytest.raises(RuntimeError, match="derived input"):
            list(store._files.ordered_rows(interrupted(), byte_limit=128 * 1024))

    with _open(root, storage_factory=_deny_primary_chunk_bodies(sql_core_storage)) as store:
        for _ in range(3):
            assert store.usage.members == 1
            assert store.session_bound
        assert store._files._one("SELECT count(*) FROM primary_order_rows") == (len(rows),)
        statements = []
        store._files._connection.set_trace_callback(statements.append)
        pending = 0
        for _ in range(len(rows) + 2):
            statements.clear()
            if store.recover_step():
                break
            pending += 1
            assert sum(s.startswith("DELETE FROM primary_order_rows") for s in statements) <= 1
            assert store._files.ordering_reserved_bytes() == 128 * 1024
        else:
            pytest.fail("bounded derived recovery did not finish")
        assert pending == len(rows) + 1
        assert store._files.ordering_reserved_bytes() == 0
        assert store.load_controls(_member()).prepared == original.prepared


def test_explicit_recovery_keeps_ready_ordering_admitted(tmp_path, sql_core_storage):
    root = tmp_path.resolve() / "state.sqlite3"
    with _open(root, initialize=True, storage_factory=sql_core_storage) as store:
        list(store._files.ordered_rows([("a", 1, "a" * 64)], byte_limit=128 * 1024))
    with _open(root, storage_factory=sql_core_storage) as store:
        assert store.recover_step()
        assert store._files.ordering_reserved_bytes() == 128 * 1024
        assert store._files._one("SELECT phase FROM primary_ordering") == ("ready",)


@pytest.mark.parametrize("role", ["prepared", "decision-accepted", "custody", "reclaimed"])
@pytest.mark.parametrize("phase", ["warming", "readiness"])
@pytest.mark.parametrize("committed", [False, True])
def test_shared_sql_resource_preparation_uncertainty_coldly_rolls_back_without_replacing_primary(
    tmp_path, sql_core_storage, role, phase, committed
):
    from tests.support.prepared_primary_resource_recovery import witness

    evidence = witness(
        tmp_path.resolve() / "state.sqlite3",
        sql_core_storage,
        role=role,
        phase=phase,
        committed=committed,
    )
    assert evidence["survivor_exact"] and evidence["member_grant_charged_once"]


@pytest.mark.parametrize("interruption", ["defer", "exception"])
def test_member_preparation_defers_authorship_and_cold_resumes_without_double_charge(
    tmp_path, sql_core_storage, interruption
):
    from functools import partial

    from riverhog_client import PrimaryStateBusy
    from riverhog_client._prepared_primary_factory import sqlite_primary_preparation_step
    from riverhog_client._prepared_primary_sqlite_preparation import SqliteReadinessPlan

    root = tmp_path / "prepared-member.sqlite3"
    member = _member(2)
    calls = []
    plan = SqliteReadinessPlan((8201, 16), 65536, 8, 192, 512 * 1024)
    execute = partial(sqlite_primary_preparation_step, plan=plan)

    def author(_grant):
        calls.append(True)
        return _author(member)

    def interrupted(storage):
        assert not execute(storage)
        if interruption == "exception":
            raise RuntimeError("member preparation interrupted")
        return False

    with _open(root, initialize=True, storage_factory=sql_core_storage) as store:
        survivor = store.prepare_or_load(_member(), REQUEST, lambda _grant: _author())
        usage = store.usage
        error = RuntimeError if interruption == "exception" else PrimaryStateBusy
        with pytest.raises(error, match="preparation"):
            store.prepare_or_load(member, REQUEST, author, storage_preparation=interrupted)
        assert not calls and store._usage == usage
        cursor = store._files.progress_journal()
        assert cursor.pad_rows == 1
        assert store._files.read_control("transition", byte_limit=LIMITS.control_bytes)

    with _open(root, storage_factory=sql_core_storage) as store:
        assert store._files.progress_journal() == cursor and store.usage == usage
        for _ in range(512):
            try:
                selected = store.prepare_or_load(
                    member, REQUEST, author, storage_preparation=execute
                )
            except PrimaryStateBusy:
                assert not calls and store.usage == usage
            else:
                break
        else:
            pytest.fail("interrupted member preparation did not finish")
        assert calls == [True]
        assert store.load(_member()) == survivor
        assert store.usage.members == usage.members + 1
        assert store.usage.sequence == usage.sequence + 1
        assert store.usage.control_bytes == usage.control_bytes + LIMITS.member_reservation
        settled = store.usage
    with _open(root, storage_factory=sql_core_storage) as store:
        assert store.load(member) == selected
        assert store.load(_member()) == survivor
        assert store.prepare_or_load(member, REQUEST, _no_author) == selected
        assert store.usage == settled
