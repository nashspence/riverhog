"""Reusable content-opaque direct-to-final Riverhog collection producer."""

from __future__ import annotations

import builtins
import hashlib
import itertools
import tempfile
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, replace
from dataclasses import field as dataclass_field
from pathlib import Path
from typing import Any, BinaryIO

from pydantic import TypeAdapter
from riverhog_protocol import (
    CollectionDescription,
    CollectionTag,
)
from riverhog_protocol.artifact_identity import ArtifactId, ArtifactMemberIdentityDocument
from riverhog_protocol.collection_completion import CollectionCompletionRequirementDocument
from riverhog_protocol.collection_upload_transport import (
    CollectionUploadArtifactCustodyReceiptDocument,
    CollectionUploadRegistrationConstraintsDocument,
    CollectionUploadUnitWorkDocument,
    validate_collection_upload_artifact_custody_receipt,
)
from riverhog_protocol.errors import NotFound
from riverhog_protocol.paths import CollectionId
from riverhog_protocol.provenance_transport import MaterializationHintDocument
from riverhog_protocol.storage_names import ArchiveStoreName
from riverhog_provenance import BoundedSourceObserver, ObservationResult, StreamSource
from time_formats import parse_utc_timestamp, utc_epoch_ns_now

from riverhog_client.canonical_production import (
    ProducerAttribution,
    bind_produced_member,
    member_materialization_decision,
)
from riverhog_client.client import ApiClient
from riverhog_client.initial_tags import create_or_resume_with_initial_collection_tags
from riverhog_client.source_hashing import RawSourceHash, hash_raw_source_chunks
from riverhog_client.uploads import (
    configured_upload_concurrency,
    configured_upload_window,
    upload_collection_units,
)

ReadProgress = Callable[[str, int, int], None]
RangeReader = Callable[[int, int], bytes]
_STREAM_VERIFY_BLOCK_BYTES = 8 * 1024 * 1024
COLLECTION_UPLOAD_REGISTRATION_BATCH_FILES = 16
_CUSTODY_POLL_BATCH_FILES = 16


@dataclass(frozen=True, slots=True)
class ProducerFile:
    source: Path
    artifact_id: ArtifactId
    materialization_hint: tuple[str, ...] | None = None
    allow_missing_materialization_hint: bool = False
    observation: ObservationResult | None = None
    output_id: str | None = None
    causal_input_states: tuple[Mapping[str, Any], ...] = ()

    def __post_init__(self) -> None:
        supplied = self.source
        if supplied.is_symlink():
            raise ValueError(f"producer source must not be a symlink: {supplied}")
        resolved = supplied.resolve()
        object.__setattr__(self, "source", resolved)
        object.__setattr__(self, "artifact_id", ArtifactId(self.artifact_id))
        if not resolved.is_file():
            raise ValueError(f"producer source must be a real regular file: {resolved}")
        _validate_hint_decision(self.materialization_hint, self.allow_missing_materialization_hint)


@dataclass(frozen=True, slots=True)
class ProducerStream:
    """Pre-identified range-readable producer input.

    The reader must return exactly ``size`` bytes for every valid range. The
    producer verifies the complete declared SHA-256 before registering the
    artifact with Riverhog and revalidates every block used during upload. A
    target may therefore expose a file, object, or generated random-access source
    without sharing its filesystem with the coordinator.
    """

    artifact_id: ArtifactId
    bytes: int
    sha256: str
    read_range: RangeReader
    materialization_hint: tuple[str, ...] | None = None
    allow_missing_materialization_hint: bool = False
    observation: ObservationResult | None = None
    output_id: str | None = None
    causal_input_states: tuple[Mapping[str, Any], ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "artifact_id", ArtifactId(self.artifact_id))
        if isinstance(self.bytes, bool) or not isinstance(self.bytes, int) or self.bytes < 0:
            raise ValueError("producer stream byte count must be non-negative")
        digest = self.sha256.casefold()
        if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
            raise ValueError("producer stream SHA-256 is invalid")
        object.__setattr__(self, "sha256", digest)
        if not callable(self.read_range):
            raise ValueError("producer stream requires a range reader")
        _validate_hint_decision(self.materialization_hint, self.allow_missing_materialization_hint)


ProducerInput = ProducerFile | ProducerStream


@dataclass(frozen=True, order=True, slots=True)
class ProducerArtifactIdentity:
    """Exact artifact identity established by the producer's verification pass."""

    artifact_id: ArtifactId
    bytes: int
    sha256: str


@dataclass(frozen=True, slots=True)
class ProducedCollection:
    collection_id: CollectionId
    archive_root_sha256: str
    artifact_set_identity: str
    receipt: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ProducerArtifactCustody:
    """Exact Riverhog safe-release receipt for a completed producer artifact."""

    artifact: ProducerArtifactIdentity
    receipt: CollectionUploadArtifactCustodyReceiptDocument


@dataclass(frozen=True, slots=True)
class _Source:
    artifact_id: ArtifactId
    bytes: int
    sha256: str
    materialization_hint: tuple[str, ...] | None
    allow_missing_materialization_hint: bool
    observation: ObservationResult | None = None
    output_id: str | None = None
    causal_input_states: tuple[Mapping[str, Any], ...] = ()
    content: builtins.bytes | None = None
    raw_parts: dict[str, object] | None = None
    raw_digest_spool: RawSourceHash | None = None
    reader: RangeReader | None = None

    def read_range(self, offset: int, size: int) -> builtins.bytes:
        if offset < 0 or size < 0 or offset + size > self.bytes:
            raise RuntimeError(f"upload unit requested an invalid source range: {self.artifact_id}")
        if self.content is not None:
            return self.content[offset : offset + size]
        if self.reader is not None:
            content = self.reader(offset, size)
            if len(content) != size:
                raise RuntimeError(
                    f"producer source returned an incomplete range: {self.artifact_id}"
                )
            return content
        raise RuntimeError(f"producer source has no readable content: {self.artifact_id}")

    def close(self) -> None:
        if self.raw_digest_spool is not None:
            self.raw_digest_spool.close()
        if isinstance(self.reader, _VerifiedRangeReader):
            self.reader.close()


class CollectionProducer:
    """Stream protocol-complete files into one finalized Riverhog collection.

    The calling adapter or transform target retains its source bytes until this
    method returns a finalized receipt. Reusing the same idempotency key safely
    reconciles lost responses and process restarts. The producer retains only
    the server-declared open pack window; Riverhog owns the complete membership.
    """

    def __init__(
        self,
        api: ApiClient,
        *,
        producer_app: str,
        adapter_id: str,
        adapter_version: str,
        ingest_source: str,
        archive_store: ArchiveStoreName | None = None,
        use_cache: bool | None = None,
        copy_to: Sequence[ArchiveStoreName] | None = None,
        description: CollectionDescription | None = None,
        tags: Sequence[CollectionTag] = (),
    ) -> None:
        self.api = api
        self.producer_app = producer_app
        self.adapter_id = adapter_id
        self.adapter_version = adapter_version
        self.ingest_source = ingest_source
        self.archive_store = archive_store
        self.use_cache = use_cache
        self.copy_to = tuple(copy_to) if copy_to is not None else None
        self.description = description
        self.tags = tuple(tags)

    def publish(
        self,
        files: Iterable[ProducerFile],
        *,
        source_event_id: str,
        source_context: Mapping[str, object] | None = None,
        provenance_journals: Iterable[tuple[str, bytes]] | None = None,
        idempotency_key: str | None = None,
        event_context: Mapping[str, object] | None = None,
        poll_seconds: float = 2.0,
        timeout_seconds: float = 24 * 60 * 60,
        progress: ReadProgress | None = None,
    ) -> ProducedCollection:
        return self.publish_inputs(
            files,
            source_event_id=source_event_id,
            source_context=source_context,
            provenance_journals=provenance_journals,
            idempotency_key=idempotency_key,
            event_context=event_context,
            poll_seconds=poll_seconds,
            timeout_seconds=timeout_seconds,
            progress=progress,
        )

    def publish_inputs(
        self,
        files: Iterable[ProducerInput],
        *,
        source_event_id: str,
        source_context: Mapping[str, object] | None = None,
        provenance_journals: Iterable[tuple[str, bytes]] | None = None,
        idempotency_key: str | None = None,
        event_context: Mapping[str, object] | None = None,
        poll_seconds: float = 2.0,
        timeout_seconds: float = 24 * 60 * 60,
        progress: ReadProgress | None = None,
    ) -> ProducedCollection:
        source_inputs = iter(files)
        try:
            first_input = next(source_inputs)
        except StopIteration as exc:
            raise ValueError("producer collection must contain at least one source file") from exc
        producer = IncrementalCollectionProducer(
            self.api,
            producer_app=self.producer_app,
            adapter_id=self.adapter_id,
            adapter_version=self.adapter_version,
            ingest_source=self.ingest_source,
            source_event_id=source_event_id,
            source_context=source_context,
            idempotency_key=idempotency_key,
            archive_store=self.archive_store,
            use_cache=self.use_cache,
            copy_to=self.copy_to,
            description=self.description,
            tags=self.tags,
            event_context=event_context,
            progress=progress,
        )
        try:
            producer.stage_provenance_journals(provenance_journals or ())
            batch: list[ProducerInput] = []
            for item in itertools.chain((first_input,), source_inputs):
                batch.append(item)
                if len(batch) == COLLECTION_UPLOAD_REGISTRATION_BATCH_FILES:
                    producer.append_inputs(batch)
                    batch.clear()
            if batch:
                producer.append_inputs(batch)
            return producer.finish(poll_seconds=poll_seconds, timeout_seconds=timeout_seconds)
        finally:
            producer.stop()


class IncrementalCollectionProducer:
    """Transfer finalized artifacts into one resumable Riverhog construction session.

    A payload seal lets this client discard its range-reader state. The caller
    retains each original source until Riverhog returns a custody receipt that
    binds the verified payload, exact primary and required input history. Completion
    remains explicit and is the sole path that publishes a collection.
    """

    def __init__(
        self,
        api: ApiClient,
        *,
        producer_app: str,
        adapter_id: str,
        adapter_version: str,
        ingest_source: str,
        source_event_id: str,
        source_context: Mapping[str, object] | None = None,
        idempotency_key: str | None = None,
        archive_store: ArchiveStoreName | None = None,
        use_cache: bool | None = None,
        copy_to: Sequence[ArchiveStoreName] | None = None,
        description: CollectionDescription | None = None,
        tags: Sequence[CollectionTag] = (),
        event_context: Mapping[str, object] | None = None,
        progress: ReadProgress | None = None,
        completion_requirement: CollectionCompletionRequirementDocument | None = None,
    ) -> None:
        self.api = api
        self.progress = progress
        construction_identity = hashlib.sha256(
            (idempotency_key or source_event_id).encode("utf-8")
        ).hexdigest()
        self._attribution = ProducerAttribution(
            producer_app=producer_app,
            adapter_id=adapter_id,
            adapter_version=adapter_version,
            source_event_id=source_event_id,
            ingest_source=ingest_source,
            source_context=dict(source_context or {}),
            construction_identity=construction_identity,
            completion_requirement=completion_requirement,
        )
        self._sources: dict[ArtifactId, _Source] = {}
        # Only payload-sealed (or not yet classified) members can gain custody
        # without another upload. Keep a fair, bounded receipt polling frontier.
        self._custody_candidates: OrderedDict[ArtifactId, None] = OrderedDict()
        self._unsealed_sources: OrderedDict[ArtifactId, None] = OrderedDict()
        self._pending_source_resolver: Callable[[ArtifactId], ProducerInput] | None = None
        self._restored_sources: OrderedDict[ArtifactId, _Source] = OrderedDict()
        self._source_lock = threading.RLock()
        self._closed = False
        self._needs_upload_scan = True
        self._heartbeat_stop = threading.Event()
        self._heartbeat_failure: BaseException | None = None
        self._heartbeat_thread: threading.Thread | None = None
        self._finalized: ProducedCollection | None = None
        self.constraints: CollectionUploadRegistrationConstraintsDocument | None
        session = create_or_resume_with_initial_collection_tags(
            tags,
            create_or_resume=lambda first_batch, identity: (
                api.create_or_resume_collection_upload_session(
                    idempotency_key or construction_identity,
                    ingest_source=ingest_source,
                    description=description,
                    tags=first_batch,
                    initial_tag_set_identity=identity,
                    archive_store=archive_store,
                    use_cache=use_cache,
                    copy_to=copy_to,
                    event_context=event_context,
                    custody_mode="custody-transfer",
                )
            ),
            add_tags=lambda collection_id, batch: api.add_collection_upload_session_tags(
                collection_id, batch
            ),
        )
        self._heartbeat_interval_seconds = _custody_heartbeat_interval(session)
        self.resumed = bool(session.get("resumed"))
        self.collection_id: int = TypeAdapter(CollectionId).validate_python(
            session.get("collection_id")
        )
        delivery_context_id = session.get("delivery_context_id")
        if not isinstance(delivery_context_id, str):
            raise RuntimeError("Riverhog upload session omitted its delivery context")
        self.delivery_context_id = delivery_context_id
        if str(session.get("state") or "") == "finalized":
            self._finalized = _finalized_receipt(session)
            self._closed = True
            self.constraints = None
            return
        self.construction_identity_sha256 = session.get("construction_identity_sha256")
        if completion_requirement is not None:
            if not isinstance(self.construction_identity_sha256, str):
                raise RuntimeError("execution construction omitted its accepted creation identity")
            self._attribution = replace(
                self._attribution, construction_identity=self.construction_identity_sha256
            )
            api.set_collection_upload_session_completion_requirement(
                self.collection_id, completion_requirement
            )
        constraints = session.get("registration_constraints")
        if not isinstance(constraints, Mapping):
            raise RuntimeError("Riverhog upload session did not return registration constraints")
        self.constraints = CollectionUploadRegistrationConstraintsDocument.model_validate(
            dict(constraints)
        )
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name=f"riverhog-upload-lease-{self.collection_id}",
            daemon=True,
        )
        self._heartbeat_thread.start()

    def heartbeat(self) -> None:
        if not self._closed:
            self.api.heartbeat_collection_upload_session(self.collection_id)
            self._heartbeat_failure = None

    def stage_provenance_journals(self, journals: Iterable[tuple[str, bytes]]) -> None:
        """Stage a bounded stream of exact provenance journals before sealing."""

        for journal_id, content in journals:
            self._stage_journals({str(journal_id): bytes(content)})

    def stop(self) -> None:
        self._heartbeat_stop.set()
        thread = self._heartbeat_thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=5.0)
        self._heartbeat_thread = None
        for source in self._sources.values():
            source.close()
        self._sources.clear()
        self._custody_candidates.clear()
        self._unsealed_sources.clear()
        for source in self._restored_sources.values():
            source.close()
        self._restored_sources.clear()

    def set_pending_source_resolver(self, resolver: Callable[[ArtifactId], ProducerInput]) -> None:
        """Restore exact pending readers on demand from caller-owned restart state."""
        self._pending_source_resolver = resolver

    def append_inputs(
        self,
        inputs: Sequence[ProducerInput],
        *,
        provenance_journals: Mapping[str, bytes] | None = None,
        expected_identities: Mapping[ArtifactId, ProducerArtifactIdentity] | None = None,
    ) -> tuple[ProducerArtifactCustody, ...]:
        if self._closed or self.constraints is None:
            raise RuntimeError("incremental collection producer is already closed")
        self._require_heartbeat()
        if not inputs:
            return ()
        supplied_ids = [item.artifact_id for item in inputs]
        if len(supplied_ids) != len(set(supplied_ids)):
            raise ValueError("incremental producer artifact IDs must be unique")
        normalized_journals = {
            str(key): bytes(value) for key, value in (provenance_journals or {}).items()
        }
        expected = {
            ArtifactId(key): identity for key, identity in (expected_identities or {}).items()
        }
        if expected and set(expected) != set(supplied_ids):
            raise ValueError("expected producer identities must match the supplied artifact IDs")
        self._stage_journals(normalized_journals)
        candidates: list[_Source] = []
        receipts: list[ProducerArtifactCustody] = []
        for item in inputs:
            expected_identity = expected.get(item.artifact_id)
            source = (
                _hash_local_source(
                    item,
                    pack_member_bytes=self.constraints.pack_member_bytes,
                    raw_part_bytes=self.constraints.raw_part_plaintext_bytes,
                    progress=self.progress,
                )
                if isinstance(item, ProducerFile)
                else _verify_stream_source(
                    item,
                    pack_member_bytes=self.constraints.pack_member_bytes,
                    raw_part_bytes=self.constraints.raw_part_plaintext_bytes,
                    progress=self.progress,
                )
            )
            if (
                expected_identity is not None
                and ProducerArtifactIdentity(
                    source.artifact_id,
                    source.bytes,
                    source.sha256,
                )
                != expected_identity
            ):
                raise ValueError(
                    f"producer source differs from its expected identity: {item.artifact_id}"
                )
            candidates.append(source)
        receipts.extend(self._append_sources(candidates))
        if self._needs_upload_scan:
            receipts.extend(self._upload_available())
        return tuple(receipts)

    def finish(
        self,
        *,
        provenance_journals: Mapping[str, bytes] | None = None,
        poll_seconds: float = 2.0,
        timeout_seconds: float = 24 * 60 * 60,
    ) -> ProducedCollection:
        if self._finalized is not None:
            return self._finalized
        if self._closed or self.constraints is None:
            raise RuntimeError("incremental collection producer is already closed")
        self._require_heartbeat()
        self._stage_journals(
            {str(key): bytes(value) for key, value in (provenance_journals or {}).items()}
        )
        if self._needs_upload_scan:
            self._upload_available()
        receipt = self.api.complete_collection_upload_session(self.collection_id)
        if str(receipt.get("state") or "") != "finalized":
            self._upload_available(reconcile=False)
        deadline = time.monotonic() + timeout_seconds
        while str(receipt.get("state") or "") != "finalized":
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Riverhog collection {self.collection_id} did not finalize before the timeout"
                )
            time.sleep(max(0.05, poll_seconds))
            receipt = self.api.get_collection_upload_session(self.collection_id)
        self._closed = True
        self.stop()
        self._finalized = _finalized_receipt(receipt)
        return self._finalized

    def _heartbeat_loop(self) -> None:
        api = self.api.spawn()
        try:
            retry_seconds = self._heartbeat_interval_seconds
            while not self._heartbeat_stop.wait(retry_seconds):
                try:
                    api.heartbeat_collection_upload_session(self.collection_id)
                    self._heartbeat_failure = None
                    retry_seconds = self._heartbeat_interval_seconds
                except Exception as exc:
                    self._heartbeat_failure = exc
                    retry_seconds = min(
                        10.0,
                        max(0.1, self._heartbeat_interval_seconds / 3),
                    )
        finally:
            close = getattr(api, "close", None)
            if callable(close):
                close()

    def _require_heartbeat(self) -> None:
        if self._heartbeat_failure is not None:
            try:
                self.heartbeat()
            except Exception as exc:
                raise RuntimeError("collection upload custody lease heartbeat failed") from exc

    def _append_sources(self, values: Sequence[_Source]) -> tuple[ProducerArtifactCustody, ...]:
        candidates = list(values)
        if len({source.artifact_id for source in candidates}) != len(candidates):
            raise ValueError("incremental producer artifact IDs must be unique")
        for source in candidates:
            existing = self._sources.get(source.artifact_id)
            if existing is not None and _registered_identity(existing) != _registered_identity(
                source
            ):
                raise RuntimeError(
                    f"resumed producer artifact identity changed: {source.artifact_id}"
                )
            self._sources[source.artifact_id] = source
            self._unsealed_sources.setdefault(source.artifact_id, None)
        registration = [_source_registration(source) for source in candidates]
        constraints = self.constraints
        if constraints is None:
            raise RuntimeError("incremental collection producer has no registration constraints")
        receipts: list[ProducerArtifactCustody] = []
        for start in range(0, len(registration), COLLECTION_UPLOAD_REGISTRATION_BATCH_FILES):
            source_batch = candidates[start : start + COLLECTION_UPLOAD_REGISTRATION_BATCH_FILES]
            registered = self.api.register_collection_upload_session_artifacts(
                self.collection_id,
                registration[start : start + COLLECTION_UPLOAD_REGISTRATION_BATCH_FILES],
                registration_constraints=constraints,
            )
            for source in source_batch:
                _register_source_raw_digests(self.api, self.collection_id, source)
                try:
                    accepted = self.api.get_collection_upload_session_artifact_provenance_binding(
                        self.collection_id, source.artifact_id
                    )
                except NotFound:
                    accepted = None
                if accepted is None:
                    observation = source.observation or BoundedSourceObserver().observe(
                        StreamSource(
                            _SequentialRangeReader(source),
                            expected_length=source.bytes,
                        )
                    )
                    bind_produced_member(
                        self.api,
                        collection_id=self.collection_id,
                        member=ArtifactMemberIdentityDocument.model_validate(
                            {
                                "artifact_id": source.artifact_id,
                                "bytes": str(source.bytes),
                                "sha256": source.sha256,
                            }
                        ),
                        observation=observation,
                        delivery_context_id=self.delivery_context_id,
                        attribution=self._attribution,
                        materialization_hint=source.materialization_hint,
                        allow_missing_materialization_hint=(
                            source.allow_missing_materialization_hint
                        ),
                        output_id=source.output_id,
                        causal_input_states=source.causal_input_states,
                    )
                else:
                    if accepted.artifact_id != source.artifact_id:
                        raise RuntimeError(
                            "Riverhog returned another artifact's provenance binding"
                        )
                    self.api.set_collection_upload_session_materialization_decisions(
                        self.collection_id,
                        member_materialization_decision(
                            artifact_id=source.artifact_id,
                            materialization_hint=source.materialization_hint,
                            allow_missing_materialization_hint=(
                                source.allow_missing_materialization_hint
                            ),
                        ),
                    )
            rows = registered.get("artifacts")
            if not isinstance(rows, list):
                raise RuntimeError("Riverhog returned invalid registered artifacts")
            receipts.extend(self._accept_registered_rows(iter(rows), expected=source_batch))
            volumes = registered.get("volumes")
            if isinstance(volumes, list) and volumes:
                self._needs_upload_scan = True
        return tuple(receipts)

    def _upload_available(self, *, reconcile: bool = True) -> tuple[ProducerArtifactCustody, ...]:
        constraints = self.constraints
        if constraints is None:
            raise RuntimeError("incremental producer has no registration constraints")
        concurrency = configured_upload_concurrency()

        def content_for_unit(unit: CollectionUploadUnitWorkDocument) -> bytes:
            chunks: list[bytes] = []
            for row in unit.sources:
                with self._source_lock:
                    source = self._sources.get(row.artifact_id)
                    if source is None or (source.reader is None and source.content is None):
                        source = self._restored_sources.get(row.artifact_id)
                        if source is None and self._pending_source_resolver is not None:
                            item = self._pending_source_resolver(row.artifact_id)
                            if item.artifact_id != row.artifact_id:
                                raise ValueError("pending source resolver changed the artifact ID")
                            source = (
                                _hash_local_source(
                                    item,
                                    pack_member_bytes=0,
                                    raw_part_bytes=constraints.raw_part_plaintext_bytes,
                                    progress=self.progress,
                                )
                                if isinstance(item, ProducerFile)
                                else _verify_stream_source(
                                    item,
                                    pack_member_bytes=0,
                                    raw_part_bytes=constraints.raw_part_plaintext_bytes,
                                    progress=self.progress,
                                )
                            )
                            self._restored_sources[row.artifact_id] = source
                            while len(self._restored_sources) > 8:
                                _, evicted = self._restored_sources.popitem(last=False)
                                evicted.close()
                        elif source is not None:
                            self._restored_sources.move_to_end(row.artifact_id)
                    if source is None:
                        raise RuntimeError(
                            f"uncustodied producer source is unavailable: {row.artifact_id}"
                        )
                    if row.artifact_sha256 != source.sha256:
                        raise RuntimeError(
                            f"Riverhog requested a changed producer artifact: {row.artifact_id}"
                        )
                    chunks.append(source.read_range(row.offset, row.bytes))
            return b"".join(chunks)

        upload_collection_units(
            self.api,
            self.collection_id,
            content_for_unit=content_for_unit,
            concurrency=concurrency,
            window=configured_upload_window(concurrency=concurrency),
            client_factory=self.api.spawn,
        )
        receipts = (
            (*self._reconcile_pending_sources(), *self.reconcile_custody()) if reconcile else ()
        )
        self._needs_upload_scan = False
        return receipts

    def _reconcile_pending_sources(self) -> tuple[ProducerArtifactCustody, ...]:
        receipts: list[ProducerArtifactCustody] = []
        constraints = self.constraints
        if constraints is None:
            raise RuntimeError("incremental collection producer has no registration constraints")
        # Previously sealed members only need read-only receipt polling. Do not
        # re-register their growing prefix whenever a later pack/raw unit seals.
        pending = [
            self._sources[artifact_id]
            for artifact_id in self._unsealed_sources
            if artifact_id in self._sources
        ]
        for start in range(0, len(pending), COLLECTION_UPLOAD_REGISTRATION_BATCH_FILES):
            source_batch = pending[start : start + COLLECTION_UPLOAD_REGISTRATION_BATCH_FILES]
            payload = self.api.register_collection_upload_session_artifacts(
                self.collection_id,
                [_source_registration(source) for source in source_batch],
                registration_constraints=constraints,
            )
            rows = payload.get("artifacts")
            if not isinstance(rows, list):
                raise RuntimeError("Riverhog returned invalid registered artifacts")
            receipts.extend(self._accept_registered_rows(iter(rows), expected=source_batch))
        return tuple(receipts)

    def reconcile_custody(self) -> tuple[ProducerArtifactCustody, ...]:
        """Poll a bounded, fair batch of members that can gain full custody.

        An unsealed open-pack member cannot gain custody just because another
        output's history was accepted. Upload reconciliation refreshes that
        frontier when payload work completes. A seal is never a safe-release
        receipt: every returned receipt still passes exact identity, history
        and completion-requirement validation.
        """
        self._require_heartbeat()
        if self.constraints is None:
            raise RuntimeError("incremental collection producer has no registration constraints")
        receipts: list[ProducerArtifactCustody] = []
        for _ in range(min(len(self._custody_candidates), _CUSTODY_POLL_BATCH_FILES)):
            artifact_id = next(iter(self._custody_candidates))
            source = self._sources.get(artifact_id)
            if source is None:
                self._custody_candidates.pop(artifact_id)
                continue
            # Leave the member queued if the read or receipt validation fails.
            # Reads do not re-register metadata or acquire an upload write lock.
            row = self.api.get_collection_upload_session_artifact(self.collection_id, artifact_id)
            receipts.extend(self._accept_registered_rows(iter((row,)), expected=(source,)))
            if artifact_id in self._custody_candidates:
                self._custody_candidates.move_to_end(artifact_id)
        return tuple(receipts)

    def resume_artifact_custody(
        self, identity: ProducerArtifactIdentity
    ) -> ProducerArtifactCustody | None:
        """Reconcile one previously verified identity without rereading released bytes."""
        self._require_heartbeat()
        source = _Source(
            artifact_id=identity.artifact_id,
            bytes=identity.bytes,
            sha256=identity.sha256,
            materialization_hint=None,
            allow_missing_materialization_hint=True,
        )
        member = self.api.get_collection_upload_session_artifact(
            self.collection_id, identity.artifact_id
        )
        receipts = self._accept_registered_rows(iter((member,)), expected=(source,))
        return receipts[0] if receipts else None

    def _accept_registered_rows(
        self,
        rows: Iterator[dict[str, Any]],
        *,
        expected: Sequence[_Source],
    ) -> tuple[ProducerArtifactCustody, ...]:
        expected_by_id = {source.artifact_id: source for source in expected}
        receipts: list[ProducerArtifactCustody] = []
        for row in rows:
            source = _Source(
                artifact_id=ArtifactId(str(row.get("artifact_id") or "")),
                bytes=int(row.get("bytes") or 0),
                sha256=str(row.get("sha256") or ""),
                materialization_hint=None,
                allow_missing_materialization_hint=True,
            )
            expected_source = expected_by_id.pop(source.artifact_id, None)
            if expected_source is None:
                raise RuntimeError(
                    f"Riverhog returned an unexpected artifact: {source.artifact_id}"
                )
            if _source_identity(expected_source) != _source_identity(source):
                raise RuntimeError(f"Riverhog changed a registered artifact: {source.artifact_id}")
            receipt_value = row.get("custody_receipt")
            if receipt_value is not None:
                receipt = CollectionUploadArtifactCustodyReceiptDocument.model_validate(
                    receipt_value
                )
                validate_collection_upload_artifact_custody_receipt(
                    self.collection_id,
                    ArtifactMemberIdentityDocument.model_validate(
                        {
                            "artifact_id": source.artifact_id,
                            "bytes": str(source.bytes),
                            "sha256": source.sha256,
                        }
                    ),
                    receipt,
                )
                requirement = self._attribution.completion_requirement
                expected_requirement = None if requirement is None else requirement.identity
                if receipt.completion_requirement_sha256 != expected_requirement:
                    raise RuntimeError(
                        "Riverhog custody receipt changed the completion requirement"
                    )
                receipts.append(
                    ProducerArtifactCustody(
                        artifact=ProducerArtifactIdentity(
                            source.artifact_id, source.bytes, source.sha256
                        ),
                        receipt=receipt,
                    )
                )
                self._custody_candidates.pop(source.artifact_id, None)
                self._unsealed_sources.pop(source.artifact_id, None)
                owned = self._sources.pop(source.artifact_id, None)
                if owned is not None:
                    owned.close()
            elif source.artifact_id in self._sources:
                if row.get("payload_sealed") is False:
                    self._custody_candidates.pop(source.artifact_id, None)
                    self._unsealed_sources.setdefault(source.artifact_id, None)
                else:
                    # Missing seal information is not evidence of an open
                    # pack. Conservatively keep the member eligible to poll.
                    self._custody_candidates.setdefault(source.artifact_id, None)
            if receipt_value is None and row.get("payload_sealed") is True:
                # Only the internal range reader can be released here. The
                # adapter retains its source until the full custody receipt.
                owned = self._sources.get(source.artifact_id)
                if owned is not None:
                    self._unsealed_sources.pop(source.artifact_id, None)
                    owned.close()
                    self._sources[source.artifact_id] = replace(
                        owned, reader=None, content=None, raw_digest_spool=None, observation=None
                    )
        if expected_by_id:
            raise RuntimeError("Riverhog omitted requested registered artifacts")
        return tuple(receipts)

    def _stage_journals(self, values: Mapping[str, bytes]) -> None:
        for journal_id, content in sorted(values.items()):
            self.api.upload_collection_upload_session_provenance_journal(
                self.collection_id,
                journal_id,
                content=(content,),
                byte_count=len(content),
                sha256=hashlib.sha256(content).hexdigest(),
            )


def _hash_local_source(
    item: ProducerFile,
    *,
    pack_member_bytes: int,
    raw_part_bytes: int,
    progress: ReadProgress | None,
) -> _Source:
    observed = item.source.stat()
    expected = observed.st_size
    offset = 0
    block_sha256s = _DigestSpool()

    def read_local(start: int, size: int) -> bytes:
        before = item.source.stat()
        _require_same_file(before, observed, artifact_id=item.artifact_id)
        with item.source.open("rb") as stream:
            stream.seek(start)
            content = stream.read(size)
        after = item.source.stat()
        _require_same_file(after, observed, artifact_id=item.artifact_id)
        if len(content) != size:
            raise RuntimeError(f"producer source returned an incomplete range: {item.artifact_id}")
        return content

    def chunks() -> Iterator[bytes]:
        nonlocal offset
        while offset < expected:
            size = min(_STREAM_VERIFY_BLOCK_BYTES, expected - offset)
            chunk = read_local(offset, size)
            block_sha256s.append(hashlib.sha256(chunk).digest())
            offset += size
            if progress is not None:
                progress(item.artifact_id, offset, expected)
            yield chunk

    if expected >= pack_member_bytes:
        manifest = hash_raw_source_chunks(
            artifact_id=item.artifact_id,
            chunks=chunks(),
            expected_bytes=expected,
            part_plaintext_bytes=raw_part_bytes,
        )
        sha256 = manifest.summary.sha256
        raw_parts: dict[str, object] | None = {
            "part_plaintext_bytes": str(manifest.summary.part_plaintext_bytes),
            "part_count": str(manifest.summary.part_count),
            "ordered_sha256": manifest.summary.ordered_part_sha256,
        }
        raw_digest_spool: RawSourceHash | None = manifest
    else:
        digest = hashlib.sha256()
        for chunk in chunks():
            digest.update(chunk)
        sha256 = digest.hexdigest()
        raw_parts = None
        raw_digest_spool = None
    _require_same_file(item.source.stat(), observed, artifact_id=item.artifact_id)
    return _Source(
        artifact_id=item.artifact_id,
        bytes=expected,
        sha256=sha256,
        materialization_hint=item.materialization_hint,
        allow_missing_materialization_hint=item.allow_missing_materialization_hint,
        observation=item.observation,
        output_id=item.output_id,
        causal_input_states=item.causal_input_states,
        raw_parts=raw_parts,
        raw_digest_spool=raw_digest_spool,
        reader=_VerifiedRangeReader(
            source=read_local,
            artifact_id=item.artifact_id,
            bytes=expected,
            block_sha256s=block_sha256s,
        ),
    )


def _require_same_file(current: object, expected: object, *, artifact_id: ArtifactId) -> None:
    for attribute in ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns"):
        if getattr(current, attribute) != getattr(expected, attribute):
            raise RuntimeError(f"producer source changed during upload verification: {artifact_id}")


def _verify_stream_source(
    item: ProducerStream,
    *,
    pack_member_bytes: int,
    raw_part_bytes: int,
    progress: ReadProgress | None,
) -> _Source:
    offset = 0
    block_sha256s = _DigestSpool()

    def chunks() -> Iterator[bytes]:
        nonlocal offset
        while offset < item.bytes:
            size = min(_STREAM_VERIFY_BLOCK_BYTES, item.bytes - offset)
            chunk = item.read_range(offset, size)
            if len(chunk) != size:
                raise RuntimeError(
                    f"producer stream returned an incomplete range: {item.artifact_id}"
                )
            block_sha256s.append(hashlib.sha256(chunk).digest())
            offset += size
            if progress is not None:
                progress(item.artifact_id, offset, item.bytes)
            yield chunk

    if item.bytes >= pack_member_bytes:
        manifest = hash_raw_source_chunks(
            artifact_id=item.artifact_id,
            chunks=chunks(),
            expected_bytes=item.bytes,
            part_plaintext_bytes=raw_part_bytes,
        )
        sha256 = manifest.summary.sha256
        raw_parts: dict[str, object] | None = {
            "part_plaintext_bytes": str(manifest.summary.part_plaintext_bytes),
            "part_count": str(manifest.summary.part_count),
            "ordered_sha256": manifest.summary.ordered_part_sha256,
        }
        raw_digest_spool: RawSourceHash | None = manifest
    else:
        digest = hashlib.sha256()
        for chunk in chunks():
            digest.update(chunk)
        sha256 = digest.hexdigest()
        raw_parts = None
        raw_digest_spool = None
    if sha256 != item.sha256:
        raise RuntimeError(f"producer stream identity changed before upload: {item.artifact_id}")
    verified_reader = _VerifiedRangeReader(
        source=item.read_range,
        artifact_id=item.artifact_id,
        bytes=item.bytes,
        block_sha256s=block_sha256s,
    )
    return _Source(
        artifact_id=item.artifact_id,
        bytes=item.bytes,
        sha256=item.sha256,
        materialization_hint=item.materialization_hint,
        allow_missing_materialization_hint=item.allow_missing_materialization_hint,
        observation=item.observation,
        output_id=item.output_id,
        causal_input_states=item.causal_input_states,
        raw_parts=raw_parts,
        raw_digest_spool=raw_digest_spool,
        reader=verified_reader,
    )


@dataclass(slots=True)
class _DigestSpool:
    """Disk-backed verification digests; source plaintext is never spooled."""

    _values: BinaryIO = dataclass_field(default_factory=lambda: tempfile.TemporaryFile(mode="w+b"))
    _count: int = 0
    _lock: threading.Lock = dataclass_field(default_factory=threading.Lock)

    def append(self, digest: bytes) -> None:
        if len(digest) != 32:
            raise ValueError("verification digest must be SHA-256")
        with self._lock:
            self._values.seek(self._count * 32)
            self._values.write(digest)
            self._count += 1

    def get(self, index: int) -> bytes:
        if index < 0 or index >= self._count:
            raise RuntimeError("producer verification digest is unavailable")
        with self._lock:
            self._values.seek(index * 32)
            value = self._values.read(32)
        if len(value) != 32:
            raise RuntimeError("producer verification digest spool is incomplete")
        return value

    def close(self) -> None:
        self._values.close()

    def __del__(self) -> None:
        self._values.close()


@dataclass(frozen=True, slots=True)
class _VerifiedRangeReader:
    source: RangeReader
    artifact_id: ArtifactId
    bytes: int
    block_sha256s: _DigestSpool

    def __call__(self, offset: int, size: int) -> builtins.bytes:
        if offset < 0 or size < 0 or offset + size > self.bytes:
            raise RuntimeError(f"producer source requested an invalid range: {self.artifact_id}")
        if size == 0:
            return b""
        first = offset // _STREAM_VERIFY_BLOCK_BYTES
        last = (offset + size - 1) // _STREAM_VERIFY_BLOCK_BYTES
        chunks: list[builtins.bytes] = []
        for block in range(first, last + 1):
            block_offset = block * _STREAM_VERIFY_BLOCK_BYTES
            block_size = min(_STREAM_VERIFY_BLOCK_BYTES, self.bytes - block_offset)
            content = self.source(block_offset, block_size)
            if len(content) != block_size:
                raise RuntimeError(
                    f"producer stream returned an incomplete verified block: {self.artifact_id}"
                )
            if hashlib.sha256(content).digest() != self.block_sha256s.get(block):
                raise RuntimeError(
                    f"producer source changed during upload verification: {self.artifact_id}"
                )
            chunks.append(content)
        combined = b"".join(chunks)
        relative = offset - first * _STREAM_VERIFY_BLOCK_BYTES
        return combined[relative : relative + size]

    def close(self) -> None:
        self.block_sha256s.close()


def _source_identity(source: _Source) -> tuple[str, int, str]:
    return source.artifact_id, source.bytes, source.sha256


def _registered_identity(source: _Source) -> tuple[str, int, str, tuple[str, ...] | None, bool]:
    return (
        *_source_identity(source),
        source.materialization_hint,
        source.allow_missing_materialization_hint,
    )


def _source_registration(source: _Source) -> dict[str, object]:
    return {
        "artifact_id": source.artifact_id,
        "bytes": str(source.bytes),
        "sha256": source.sha256,
        **({"raw_parts": source.raw_parts} if source.raw_parts is not None else {}),
    }


def _register_source_raw_digests(
    api: ApiClient,
    collection_id: CollectionId,
    source: _Source,
) -> None:
    spool = source.raw_digest_spool
    if spool is None:
        return
    for first_part, sha256s in spool.iter_batches():
        api.register_collection_upload_session_raw_part_digests(
            collection_id,
            {
                "artifact_id": source.artifact_id,
                "first_part": str(first_part),
                "sha256s": list(sha256s),
            },
        )


def _validate_hint_decision(hint: tuple[str, ...] | None, allow_missing: bool) -> None:
    if type(allow_missing) is not bool or (hint is None) != allow_missing:
        raise ValueError("producer requires a hint or an explicit missing-hint decision")
    if hint is not None:
        MaterializationHintDocument(components=list(hint))


class _SequentialRangeReader:
    """A bounded observation reader over an already verified source."""

    def __init__(self, source: _Source) -> None:
        self._source = source
        self._offset = 0

    def read(self, size: int = -1, /) -> bytes:
        remaining = self._source.bytes - self._offset
        count = remaining if size < 0 else min(size, remaining)
        if count == 0:
            return b""
        content = self._source.read_range(self._offset, count)
        self._offset += count
        return content


def _finalized_receipt(payload: Mapping[str, Any]) -> ProducedCollection:
    collection = payload.get("collection")
    if not isinstance(collection, Mapping):
        raise RuntimeError("finalized Riverhog upload has no collection receipt")
    collection_id = int(collection["id"])
    artifact_set_identity = str(
        payload.get("artifact_set_identity") or collection.get("artifact_set_identity") or ""
    )
    archive_root_sha256 = str(
        collection.get("archive_root_sha256") or payload.get("archive_root_sha256") or ""
    )
    if len(archive_root_sha256) != 64:
        raise RuntimeError("finalized Riverhog receipt has no immutable archive-root identity")
    if len(artifact_set_identity) != 64:
        raise RuntimeError("finalized Riverhog receipt has no content identity")
    return ProducedCollection(
        collection_id=collection_id,
        archive_root_sha256=archive_root_sha256,
        artifact_set_identity=artifact_set_identity,
        receipt=dict(payload),
    )


def _custody_heartbeat_interval(session: Mapping[str, object]) -> float:
    value = session.get("upload_state_expires_at")
    if not isinstance(value, str) or not value:
        return 60.0
    try:
        expires_ns = parse_utc_timestamp(value)
    except ValueError as exc:
        raise RuntimeError("Riverhog upload session returned an invalid custody expiry") from exc
    remaining = (expires_ns - utc_epoch_ns_now()) / 1_000_000_000
    if remaining <= 0:
        return 0.1
    return min(60.0, max(0.1, remaining / 3))


__all__ = [
    "CollectionProducer",
    "IncrementalCollectionProducer",
    "ProducedCollection",
    "ProducerFile",
    "ProducerArtifactIdentity",
    "ProducerArtifactCustody",
    "ProducerInput",
    "ProducerStream",
    "RangeReader",
]
