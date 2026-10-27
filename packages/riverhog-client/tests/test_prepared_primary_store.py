"""Crash and admission witnesses for the private prepared-byte store."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
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


def _author(member=None):
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
                "prefix_bytes": str(len(JOURNAL)),
                "prefix_sha256": hashlib.sha256(JOURNAL).hexdigest(),
            },
            "delivery_association_id": "urn:uuid:bbbbaaaa-1234-4234-8234-123456789abc",
        }
    )
    # Store tests own octet retention; Native authorship is qualified by its owner.
    return PrimaryPreparation(
        b"original selection", b"original graph", JOURNAL, b"exact decision", binding, None
    )


def _receipt(prepared):
    return CollectionUploadArtifactCustodyReceiptDocument.seal(
        collection_id=42,
        artifact_id=prepared.member.artifact_id,
        bytes=prepared.member.bytes,
        sha256=prepared.member.sha256,
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
                plaintext_bytes=str(len(JOURNAL)),
                plaintext_sha256=hashlib.sha256(JOURNAL).hexdigest(),
                sealed_receipt_sha256="b" * 64,
            ),
        ),
    )


def _open(root, *, initialize=False, limits=LIMITS, scope=SCOPE):
    store = PrimaryStore(
        root,
        allocation_id="existing-app-allocation",
        scope=scope,
        limits=limits,
        initialize=initialize,
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
link=os.link
replace_file=os.replace
def crash_link(source,target,*args,**kwargs):
    if "candidate-"+target==boundary: os._exit(37)
    return link(source,target,*args,**kwargs)
def crash_replace(source,target,*args,**kwargs):
    if "candidate-"+target==boundary: os._exit(37)
    return replace_file(source,target,*args,**kwargs)
os.link=crash_link
os.replace=crash_replace
def crash_put(self,name,*args,**kwargs):
    result=put(self,name,*args,**kwargs)
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
else:
    store.reclaim(member)
raise RuntimeError('crash boundary was not reached')
"""


def _crash(root, boundary, *, operation="prepare"):
    authored = _author()
    spec = {
        "operation": operation,
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
def test_process_exit_during_reclaim_never_subtracts_twice_or_requires_released_bytes(
    tmp_path, boundary
):
    root = tmp_path.resolve() / "state"
    with _open(root, initialize=True) as store:
        record = store.prepare_or_load(_member(), REQUEST, lambda _limit: _author())
        store.accept_custody(record, _receipt(record.prepared))
        store.accept_decision(record, b"exact decision")
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
