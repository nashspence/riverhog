"""Real SDK/Native/POSIX evidence for #997, not an alternative product store.

The API boundary below is an in-process fault fixture, not authenticated Core.
Original preparation, Native authoring, replay, receipt checking and reclamation
are the installed repository implementations. The optional warm-read experiment
is deliberately single-threaded: a guarded patch models a future backend-local
durability receipt while retaining every cold/unknown-write barrier. It never
changes a product source file or elides a write's durability barrier.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import time
from collections import Counter
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from unittest.mock import patch

from riverhog_client._prepared_primary_documents import (
    PrimaryScope,
    PrimarySession,
    primary_control_bytes,
)
from riverhog_client._prepared_primary_posix import PrimaryFiles
from riverhog_client._prepared_primary_publisher import (
    DeclaredPrimaryObservation,
    PrimaryPublisher,
)
from riverhog_client._prepared_primary_store import PrimaryRecord, PrimaryStore, PrimaryStoreLimits
from riverhog_client._prepared_primary_types import PrimaryStateError
from riverhog_client.canonical_production import ProducerAttribution
from riverhog_protocol import (
    ArtifactMemberIdentityDocument,
    CollectionUploadArtifactCustodyReceiptDocument,
    CollectionUploadCustodyObjectDocument,
    CollectionUploadProvenanceCustodyObjectDocument,
)
from riverhog_protocol.collection_production_provenance import collection_production_contract
from riverhog_protocol.errors import NotFound
from riverhog_provenance import BoundedSourceObserver, BytesSource, validate_journal_chunks

PAYLOAD = b"riverhog #997 original SDK evidence\n" * 16
ROLES = ("request", "observation", "graph", "journal", "decision")
CREATE = b'{"experiment":"997 exact original create inputs"}'
CONTEXT = "urn:uuid:ccccaaaa-1234-4234-8234-123456789abc"
LIMITS = PrimaryStoreLimits(
    pending_bytes=2 * 1024**2,
    total_bytes=4 * 1024**2,
    preparation_bytes=1024**2,
    control_bytes=8192,
)


def member() -> ArtifactMemberIdentityDocument:
    return ArtifactMemberIdentityDocument(
        artifact_id="1" * 64, bytes=str(len(PAYLOAD)), sha256=hashlib.sha256(PAYLOAD).hexdigest()
    )


def open_store(root: Path, *, initialize: bool = False) -> PrimaryStore:
    store = PrimaryStore(
        root,
        allocation_id="997-reference-existing-app-allocation",
        scope=PrimaryScope(service="997-fixture-service", account="997-fixture-account"),
        limits=LIMITS,
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
                delivery_context_id=CONTEXT,
                server_construction_identity_sha256="a" * 64,
                attribution_construction_identity_sha256="b" * 64,
                completion_requirement_sha256=None,
            )
        )
    return store


class LostReply(RuntimeError):
    """A precise injected boundary; durable fixture state remains inspectable."""


class PublicationApiFixture:
    """Exact typed HTTP-call stand-in; no authentication/provider claim is made."""

    def __init__(self) -> None:
        self.journals: dict[str, bytes] = {}
        self.binding = None
        self.member_exists = False
        self.before_bind = False
        self.after_decision = False
        self.stage_calls = 0
        self.bind_calls = 0
        self.decision_calls: list[Any] = []
        self.session_reads = 0

    def get_collection_upload_session(self, _collection):
        self.session_reads += 1
        return {
            "collection_id": "42",
            "state": "open",
            "delivery_context_id": CONTEXT,
            "construction_identity_sha256": "a" * 64,
        }

    def get_collection_upload_session_artifact(self, _collection, _artifact):
        if not self.member_exists:
            raise NotFound("member is absent")
        return member().model_dump(mode="json")

    def get_collection_upload_session_artifact_provenance_binding(self, *_args):
        if self.binding is None:
            raise NotFound("binding is absent")
        return self.binding

    def upload_collection_upload_session_provenance_journal(
        self, _collection, journal_id, *, content, byte_count, sha256
    ):
        raw = b"".join(content)
        assert (len(raw), hashlib.sha256(raw).hexdigest()) == (byte_count, sha256)
        assert self.journals.setdefault(journal_id, raw) == raw
        self.stage_calls += 1

    def bind_collection_upload_session_artifact_provenance(self, _collection, batch):
        self.bind_calls += 1
        if self.before_bind:
            self.before_bind = False
            raise LostReply("after original journal staging, before binding")
        assert self.binding in (None, batch.bindings[0])
        self.binding = batch.bindings[0]
        return batch

    def set_collection_upload_session_materialization_decisions(self, _collection, batch):
        if self.decision_calls:
            assert batch == self.decision_calls[0]
        self.decision_calls.append(batch)
        if self.after_decision:
            self.after_decision = False
            raise LostReply("after exact decision acceptance, before local acknowledgement")
        return batch


def prepare(publisher: PrimaryPublisher, observation, *, version: str = "1") -> PrimaryRecord:
    def no_automatic_observer():
        raise AssertionError("the declared observer selection was replaced")

    return publisher.prepare(
        member=member(),
        attribution=ProducerAttribution(
            "997-reference", "997-reference/v1", version, "event", "fixture", {}, "b" * 64
        ),
        observation=observation,
        automatic_observation=no_automatic_observer,
        materialization_hint=None,
        allow_missing_materialization_hint=True,
    )


def receipt_for(record: PrimaryRecord, **changed):
    prepared = record.prepared
    fields = {
        "collection_id": 42,
        "artifact_id": prepared.member.artifact_id,
        "bytes": prepared.member.bytes,
        "sha256": prepared.member.sha256,
        "primary": prepared.primary,
        "completion_requirement_sha256": None,
        "archive_objects": (
            CollectionUploadCustodyObjectDocument(
                volume_id="pack-" + "0" * 64, sealed_receipt_sha256="c" * 64
            ),
        ),
        "provenance_objects": (
            CollectionUploadProvenanceCustodyObjectDocument(
                object_id="997-original-primary",
                relative_path="provenance/payloads/997-original.bin.age",
                plaintext_bytes=str(prepared.primary.journal.prefix_bytes),
                plaintext_sha256=prepared.primary.journal.prefix_sha256,
                sealed_receipt_sha256="d" * 64,
            ),
        ),
    }
    fields.update(changed)
    return CollectionUploadArtifactCustodyReceiptDocument.seal(**fields)


def role_path(store: PrimaryStore, role: str) -> Path:
    assert role in ROLES
    with store._member(str(member().artifact_id), create=False) as files:
        return files.root / role


@dataclass
class WorkSample:
    counters: Counter = field(default_factory=Counter)
    elapsed_ns: int = 0

    def document(self) -> dict[str, Any]:
        counters = dict.fromkeys(
            (
                "preimage_open_attempts",
                "preimage_reader_opens",
                "preimage_read_bytes",
                "fsync_calls",
                "fsync_file",
                "fsync_directory",
                "os_write_bytes",
                "unlinks",
                "directory_scans",
                "path_iterations",
            ),
            0,
        )
        counters.update(self.counters)
        return {"elapsed_ns": self.elapsed_ns, **dict(sorted(counters.items()))}


@contextmanager
def measure_work():
    """Count observed Python calls/bytes, not SQLite C I/O or physical device work."""
    result = WorkSample()
    counts = result.counters
    descriptors: dict[int, str] = {}
    opened, fdopen, close = PrimaryFiles._open, os.fdopen, os.close
    fsync, unlink, write, sha256 = os.fsync, os.unlink, os.write, hashlib.sha256
    scandir, iterdir = os.scandir, Path.iterdir

    def tracked_open(files, name):
        counts["primary_open_attempts"] += 1
        if name in ROLES:
            counts["preimage_open_attempts"] += 1
        descriptor = opened(files, name)
        descriptors[descriptor] = name
        counts["primary_opens"] += 1
        return descriptor

    class Reader:
        def __init__(self, stream, descriptor, role):
            self.stream, self.descriptor, self.role = stream, descriptor, role

        def __enter__(self):
            self.stream.__enter__()
            return self

        def __exit__(self, *args):
            descriptors.pop(self.descriptor, None)
            return self.stream.__exit__(*args)

        def __getattr__(self, name):
            return getattr(self.stream, name)

        def read(self, *args):
            value = self.stream.read(*args)
            counts["primary_read_bytes"] += len(value)
            if self.role in ROLES:
                counts["preimage_read_bytes"] += len(value)
            return value

    def tracked_fdopen(descriptor, *args, **kwargs):
        stream = fdopen(descriptor, *args, **kwargs)
        role = descriptors.get(descriptor)
        if role in ROLES:
            counts["preimage_reader_opens"] += 1
        return stream if role is None else Reader(stream, descriptor, role)

    def tracked_close(descriptor):
        descriptors.pop(descriptor, None)
        return close(descriptor)

    def tracked_fsync(descriptor):
        counts["fsync_calls"] += 1
        kind = "directory" if stat.S_ISDIR(os.fstat(descriptor).st_mode) else "file"
        counts["fsync_" + kind] += 1
        return fsync(descriptor)

    def tracked_unlink(*args, **kwargs):
        counts["unlink_attempts"] += 1
        value = unlink(*args, **kwargs)
        counts["unlinks"] += 1
        return value

    def tracked_write(descriptor, data):
        size = write(descriptor, data)
        counts["os_write_calls"] += 1
        counts["os_write_bytes"] += size
        return size

    class Digest:
        def __init__(self, digest):
            self.digest = digest

        def __getattr__(self, name):
            return getattr(self.digest, name)

        def update(self, value):
            counts["python_sha256_bytes"] += len(value)
            return self.digest.update(value)

    def tracked_hash(value=b"", *args, **kwargs):
        counts["python_sha256_bytes"] += len(value)
        return Digest(sha256(value, *args, **kwargs))

    def tracked_scan(*args, **kwargs):
        counts["directory_scans"] += 1
        return scandir(*args, **kwargs)

    def tracked_iteration(*args, **kwargs):
        counts["path_iterations"] += 1
        return iterdir(*args, **kwargs)

    with ExitStack() as stack:
        for target, name, replacement in (
            (PrimaryFiles, "_open", tracked_open),
            (os, "fdopen", tracked_fdopen),
            (os, "close", tracked_close),
            (os, "fsync", tracked_fsync),
            (os, "unlink", tracked_unlink),
            (os, "write", tracked_write),
            (hashlib, "sha256", tracked_hash),
            (os, "scandir", tracked_scan),
            (Path, "iterdir", tracked_iteration),
        ):
            stack.enter_context(patch.object(target, name, replacement))
        started = time.perf_counter_ns()
        try:
            yield result
        finally:
            result.elapsed_ns = time.perf_counter_ns() - started


class ReadLeaseNeedsRecovery(PrimaryStateError):
    """The lease cannot clean, publish or establish an unknown write's durability."""


class WarmControlReadLease:
    """A bounded read epoch for one original member under its real process owner.

    Entry performs the existing complete control load/barriers. Subsequent reads
    retain current-owner, original-seal and exact-control checks but cannot mutate
    anything. A candidate requires explicit recovery outside the lease. Patches
    are only a single-threaded experiment; production needs backend-local state.
    """

    def __init__(self, store: PrimaryStore):
        self.store = store
        self.original: PrimaryRecord | None = None
        self._stack = ExitStack()
        self._valid = False

    def __enter__(self):
        self._stack.enter_context(self.store._mutex)
        try:
            self.original = self.store.load_controls(member())
            if self.original is None or self.store._files.exists("transition"):
                raise ReadLeaseNeedsRecovery("the selected member lacks a settled durable origin")
            self._valid = True
            child = PrimaryFiles.child

            def current(files):
                if not self._valid:
                    raise ReadLeaseNeedsRecovery("the read lease is invalidated")
                if files._owner_root is not self.store._files:
                    self._valid = False
                    raise ReadLeaseNeedsRecovery("another allocation cannot borrow this barrier")
                files.check_current()

            def read_only_child(files, name, *, initialize):
                current(files)
                if initialize:
                    self._valid = False
                    raise ReadLeaseNeedsRecovery("a read lease cannot create child state")
                return child(files, name, initialize=False)

            def no_candidates(files, names):
                current(files)
                if any(files.exists("candidate-" + name) for name in names):
                    self._valid = False
                    raise ReadLeaseNeedsRecovery("an unselected candidate needs explicit recovery")
                return True

            def no_write(*_args, **_kwargs):
                self._valid = False
                raise ReadLeaseNeedsRecovery("a read lease cannot publish, receive or reclaim")

            for name in (
                "__init__",
                "put",
                "put_bytes",
                "put_control",
                "remove",
                "remove_control",
                "remove_step",
                "prepare_controls",
                "ordered_rows",
                "recover_ordering_step",
                "ordering_cleanup_step",
            ):
                self._stack.enter_context(patch.object(PrimaryFiles, name, no_write))
            for name, replacement in (
                ("sync", current),
                ("child", read_only_child),
                ("discard_candidates", no_candidates),
            ):
                self._stack.enter_context(patch.object(PrimaryFiles, name, replacement))
            return self
        except BaseException:
            self._valid = False
            self._stack.close()
            raise

    def read(self) -> PrimaryRecord:
        try:
            if not self._valid:
                raise ReadLeaseNeedsRecovery("the read lease is invalidated")
            record = self.store.load_controls(member())
            if record != self.original:
                raise ReadLeaseNeedsRecovery("the original control generation changed")
            assert record is not None
            return record
        except BaseException:
            self._valid = False
            raise

    def __exit__(self, *args):
        self._valid = False
        return self._stack.__exit__(*args)


def run_experiment(root: Path, *, reads: int = 4) -> dict[str, Any]:
    """Exercise cold retry, strict bytes and receipt-only settlement on real source."""
    if type(reads) is not int or reads < 2:
        raise ValueError("at least two bounded comparison reads are required")
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    samples = {}
    api = PublicationApiFixture()
    observations = 0
    with patch.dict(os.environ, {"RIVERHOG_PROVENANCE_STATE_HOME": str(root / "identity")}):
        observer = BoundedSourceObserver()

        def observe():
            nonlocal observations
            observations += 1
            assert observations == 1, "retry authored another observation"
            return observer.observe(BytesSource(PAYLOAD))

        lazy = DeclaredPrimaryObservation(
            "997-reference-observer/v1", {}, observer.catalog.checkpoint_sha256(), observe
        )
        state = root / "primary"
        with measure_work() as sample:
            with open_store(state, initialize=True) as store:
                publisher = PrimaryPublisher(api, store)
                original = prepare(publisher, lazy)
                original_pending = store.usage.pending_bytes
        samples["original_prepare_and_session"] = sample.document()
        for phase, flag in (
            ("staged_before_bind", "before_bind"),
            ("decision_reply_lost", "after_decision"),
        ):
            with measure_work() as sample:
                with open_store(state) as store:
                    publisher = PrimaryPublisher(api, store)
                    loaded = prepare(publisher, lazy)
                    assert loaded.prepared == original.prepared
                    api.member_exists = True
                    setattr(api, flag, True)
                    try:
                        publisher.replay(loaded)
                    except LostReply:
                        pass
                    else:
                        raise AssertionError("the injected loss boundary was not reached")
                    assert store.load_controls(member()).decision_accepted is None
            samples[phase] = sample.document()
        with measure_work() as sample:
            store = open_store(state)
            publisher = PrimaryPublisher(api, store)
            replayed = publisher.replay(prepare(publisher, lazy))
            assert replayed.decision_accepted is not None
            assert publisher.replay(replayed) == replayed
        samples["original_decision_reconciled"] = sample.document()
        try:
            with measure_work() as sample:
                for _ in range(reads):
                    assert store.load_controls(member()) == replayed
            samples["current_warm_controls"] = sample.document()
            with measure_work() as entry:
                lease = WarmControlReadLease(store).__enter__()
            samples["proposed_read_epoch_entry"] = entry.document()
            try:
                with measure_work() as sample:
                    for _ in range(reads):
                        assert lease.read() == replayed
                samples["proposed_warm_controls"] = sample.document()
            finally:
                lease.__exit__(None, None, None)
            path = role_path(store, "journal")
        finally:
            store.close()
        # A receipt phase must not need these redundant bytes. Execution still does.
        path.write_bytes(b"damaged redundant original journal")
        with measure_work() as sample:
            with open_store(state) as store:
                loaded = store.load_controls(member())
                receipt = receipt_for(loaded)
                retained = store.accept_custody(loaded, receipt)
                assert retained.compact
                assert store.reclaim(member()) and store.reclaim(member())
                assert store.usage.pending_bytes == 0
                assert store.load_controls(member()) == retained
        samples["cold_receipt_then_reclaim_and_retry"] = sample.document()
        raw = api.journals[original.prepared.primary.journal.journal_id]
        with validate_journal_chunks(
            (raw,),
            catalog=observer.catalog.with_contracts((collection_production_contract(),)),
            expected_anchor=original.prepared.primary.journal.model_dump(mode="json"),
            require_exact_tail=True,
        ) as summary:
            assert summary.journal_sha256 == original.prepared.journal.sha256
        assert len(api.journals) == observations == 1
        assert api.stage_calls == api.bind_calls == len(api.decision_calls) == 2
        assert samples["cold_receipt_then_reclaim_and_retry"]["preimage_reader_opens"] == 0
        assert samples["proposed_warm_controls"].get("fsync_calls", 0) == 0
        return {
            "format": "riverhog-997-prepared-primary-experiment/v1",
            "scope": (
                "real SDK + Native + Linux POSIX; in-process API fault fixture; "
                "no provider or authenticated HTTP"
            ),
            "comparison_reads": reads,
            "observer_calls": observations,
            "distinct_staged_journals": len(api.journals),
            "stage_calls": api.stage_calls,
            "bind_calls": api.bind_calls,
            "exact_decision_calls": len(api.decision_calls),
            "original_preimage_bytes": original_pending,
            "original_journal_id": original.prepared.primary.journal.journal_id,
            "original_journal_sha256": original.prepared.journal.sha256,
            "original_manifest_sha256": original.prepared.manifest_sha256,
            "original_receipt_sha256": receipt.receipt_sha256,
            "samples": samples,
            "measurement_limits": (
                "Python-call observation, including observer overhead; no physical-device bytes, "
                "SQLite C I/O, power-loss, Windows qualification or product speedup claim"
            ),
        }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path, help="new experiment directory outside the checkout")
    parser.add_argument("--reads", type=int, default=4)
    args = parser.parse_args()
    print(json.dumps(run_experiment(args.root, reads=args.reads), indent=2, sort_keys=True))
