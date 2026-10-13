"""Pure append-only RFC 7464 journal operations, independent of payload storage.

A caller owns durable writes and writer arbitration. These functions return
validated bytes; no filename, global current state, database or filesystem is
assumed. Exact existing prefixes are never reserialized.
"""

from __future__ import annotations

import copy
import hashlib
from collections.abc import Iterable, Iterator, Mapping, Sequence, Set
from dataclasses import dataclass
from typing import Any, BinaryIO, cast

from riverhog_provenance_contracts import (
    ENTRY_SCHEMA,
    PROFILE,
    ContractCatalog,
    canonical_document,
    decode_document,
    require_canonical_uuid_urn,
)

from .common import fingerprint, new_id, utc_now
from .constants import JOURNAL_POLICY, PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
from .errors import ConcurrentJournalChangeError, ProvenanceValidationError
from .graph import (
    IDENTITY_TYPES,
    GraphValidation,
    _acyclic,
    graph_from_assertions,
    iter_assertions,
    time_ns,
)

RS, LF = b"\x1e", b"\n"


@dataclass(frozen=True, slots=True)
class JournalFrame:
    json_bytes: bytes

    @property
    def document(self) -> dict[str, Any]:
        return decode_document(self.json_bytes)

    @property
    def encoded(self) -> bytes:
        return RS + self.json_bytes + LF

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.json_bytes).hexdigest()

    @property
    def reference(self) -> dict[str, str]:
        document = self.document
        return {
            "entry_id": document["id"],
            "sequence": document["sequence"],
            "json_sha256": self.sha256,
        }


def iter_journal_frames(
    chunks: Iterable[bytes], *, max_entry_bytes: int = PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
) -> Iterator[JournalFrame]:
    """Bounded frame parser, including hostile chunk boundaries and torn tails."""
    if type(max_entry_bytes) is not int or max_entry_bytes < 1:
        raise ValueError("max_entry_bytes must be positive")
    buffer = bytearray()
    started = False
    seen = False
    for chunk in chunks:
        if type(chunk) is not bytes:
            raise TypeError("journal chunks must be bytes")
        offset = 0
        while offset < len(chunk):
            if not started:
                if chunk[offset : offset + 1] != RS:
                    raise ProvenanceValidationError("bytes outside RFC 7464 frames")
                started = True
                offset += 1
                continue
            stop = chunk.find(LF, offset)
            end = len(chunk) if stop == -1 else stop
            piece = chunk[offset:end]
            if RS in piece:
                raise ProvenanceValidationError("unframed record separator before line feed")
            if len(buffer) + len(piece) > max_entry_bytes:
                raise ProvenanceValidationError("journal entry exceeds the admission byte limit")
            buffer.extend(piece)
            offset = end
            if stop != -1:
                raw = bytes(buffer)
                try:
                    decode_document(raw)
                except (ValueError, TypeError) as exc:
                    raise ProvenanceValidationError(str(exc)) from exc
                yield JournalFrame(raw)
                seen = True
                buffer.clear()
                started = False
                offset += 1
    if started:
        raise ProvenanceValidationError("incomplete final journal frame")
    if not seen:
        raise ProvenanceValidationError("journal is empty")


def parse_journal(raw: bytes) -> tuple[JournalFrame, ...]:
    return tuple(iter_journal_frames((raw,)))


def encode_entry(document: Mapping[str, Any], *, catalog: ContractCatalog | None = None) -> bytes:
    selected = catalog or ContractCatalog()
    selected.validate(ENTRY_SCHEMA, dict(document))
    raw = canonical_document(document)
    if len(raw) > PROVENANCE_JOURNAL_ENTRY_BYTES_MAX:
        raise ProvenanceValidationError("journal entry exceeds the admission byte limit")
    return RS + raw + LF


def _body_assertions(document: Mapping[str, Any]) -> dict[str, Any]:
    return cast(dict[str, Any], document["body"].get("assertions", {}))


def _identity_signature(row: Mapping[str, Any]) -> bytes:
    """Fixed referent aspects cannot be redefined under the guise of correction."""
    kind = row["type"]
    fields = {
        "artifact": ("continuity_policy_uri",),
        "occurrence": ("artifact_id", "kind", "source_context_id"),
        "state": ("occurrence_id", "extent"),
        "context": ("kind",),
        "agent": ("kind",),
    }.get(kind)
    if fields is None:
        value = {k: v for k, v in row.items() if k not in ("assertion_id", "evidence")}
    else:
        value = {"id": row["id"], "type": kind, **{k: row[k] for k in fields if k in row}}
    return canonical_document(value)


@dataclass(frozen=True, slots=True)
class JournalSummary:
    journal_id: str
    frames: Sequence[JournalFrame]
    graph_validation: GraphValidation
    retracted_assertion_ids: Set[str]
    journal_sha256: str
    journal_bytes: int
    assertion_entries: Mapping[str, dict[str, str]]

    @property
    def tail(self) -> JournalFrame:
        return self.frames[-1]

    @property
    def graph(self) -> dict[str, Any]:
        return self.graph_validation.graph

    @property
    def anchor(self) -> dict[str, Any]:
        return {
            "journal_id": self.journal_id,
            "through": self.tail.reference,
            "prefix_sha256": self.journal_sha256,
            "prefix_bytes": str(self.journal_bytes),
        }

    @property
    def states(self) -> tuple[dict[str, Any], ...]:
        return tuple(self.graph_validation.view.get("states", []))

    @property
    def delivery_associations(self) -> tuple[dict[str, Any], ...]:
        return tuple(self.graph_validation.view.get("delivery_associations", []))

    @property
    def findings(self) -> Sequence[str]:
        return self.graph_validation.findings

    def materialize(self) -> dict[str, Any]:
        return {
            "format": "riverhog-provenance-materialized/v1",
            "journal": self.anchor,
            "effective_assertions": self.graph,
            "retracted_assertion_ids": sorted(self.retracted_assertion_ids),
            "unresolved_external_references": list(self.graph_validation.external_references),
            "unresolved_profiles": list(self.graph_validation.unresolved_profiles),
        }


def validate_journal_chunks(
    chunks: Iterable[bytes],
    *,
    catalog: ContractCatalog | None = None,
    require_profiles: bool = True,
    expected_anchor: Mapping[str, Any] | None = None,
    require_exact_tail: bool = False,
) -> JournalSummary:
    from .journal_store import JournalStore

    selected = catalog or ContractCatalog()
    store = JournalStore()
    journal_id: str | None = None
    previous: JournalFrame | None = None
    prefix = hashlib.sha256()
    byte_count = 0
    matched_anchor = False
    try:
        for sequence, frame in enumerate(iter_journal_frames(chunks)):
            document = frame.document
            try:
                selected.validate(ENTRY_SCHEMA, document)
                time_ns(document["recorded_at"])
            except (ValueError, TypeError) as exc:
                raise ProvenanceValidationError(f"entry {sequence}: {exc}") from exc
            if int(document["sequence"]) != sequence:
                raise ProvenanceValidationError("noncontiguous journal sequence")
            if journal_id is None:
                journal_id = document["journal_id"]
                if (
                    document["entry_kind"] != "journal_init"
                    or document["body"]["journal"]["id"] != journal_id
                ):
                    raise ProvenanceValidationError("first entry must initialize this journal")
                parent = document["body"]["journal"].get("forked_from")
                if parent and parent["journal_id"] == journal_id:
                    raise ProvenanceValidationError(
                        "independent journal fork requires a distinct journal identity"
                    )
            else:
                if document["journal_id"] != journal_id or document["entry_kind"] == "journal_init":
                    raise ProvenanceValidationError("journal identity or initialization changed")
                if previous is None or document["previous_entry"] != previous.reference:
                    raise ProvenanceValidationError(
                        "predecessor does not match exact previous JSON text"
                    )
            if document["entry_kind"] == "checkpoint":
                body = document["body"]
                if previous is None or body["covered_through"] != previous.reference:
                    raise ProvenanceValidationError(
                        "checkpoint must cover the immediately preceding entry"
                    )
                if (
                    body["prefix_sha256"] != prefix.hexdigest()
                    or int(body["prefix_bytes"]) != byte_count
                ):
                    raise ProvenanceValidationError("checkpoint prefix commitment mismatch")
            store.accept(document, frame, catalog=selected, require_profiles=require_profiles)
            previous = frame
            prefix.update(frame.encoded)
            byte_count += len(frame.encoded)
            if expected_anchor and document["id"] == expected_anchor["through"]["entry_id"]:
                candidate = {
                    "journal_id": journal_id,
                    "through": frame.reference,
                    "prefix_sha256": prefix.hexdigest(),
                    "prefix_bytes": str(byte_count),
                }
                if candidate != dict(expected_anchor):
                    raise ProvenanceValidationError("externally supplied prefix anchor mismatch")
                matched_anchor = True
        if previous is None or journal_id is None:
            raise ProvenanceValidationError("journal is empty")
        if expected_anchor and not matched_anchor:
            raise ProvenanceValidationError("journal does not contain the expected anchored prefix")
        if require_exact_tail and (
            expected_anchor is None or previous.reference != expected_anchor["through"]
        ):
            raise ProvenanceValidationError("an exact tail anchor was required")
        store.freeze()
        return JournalSummary(
            journal_id,
            store.frames,
            store.graph_validation,
            store.retired,
            prefix.hexdigest(),
            byte_count,
            store.origins,
        )
    except BaseException:
        store.close()
        raise


def validate_journal(raw: bytes, **options: Any) -> JournalSummary:
    return validate_journal_chunks((raw,), **options)


def _entry(
    *,
    journal_id: str,
    recorder_id: str,
    kind: str,
    body: Mapping[str, Any],
    sequence: int = 0,
    previous: Mapping[str, str] | None = None,
    entry_id: str | None = None,
    recorded_at: str | None = None,
    recording_context_id: str | None = None,
) -> dict[str, Any]:
    value: dict[str, Any] = {
        "$schema": ENTRY_SCHEMA,
        "profile": PROFILE,
        "schema_version": "1.0.0",
        "id": entry_id or new_id(),
        "type": "riverhog_provenance_journal_entry",
        "journal_id": journal_id,
        "sequence": str(sequence),
        "recorded_at": recorded_at or utc_now(),
        "recorded_by_agent_id": recorder_id,
        "entry_kind": kind,
        "body": copy.deepcopy(dict(body)),
    }
    if previous is not None:
        value["previous_entry"] = dict(previous)
    if recording_context_id is not None:
        value["recording_context_id"] = recording_context_id
    return value


def create_journal(
    assertions: Mapping[str, Any],
    *,
    recorded_by_agent_id: str,
    journal_id: str | None = None,
    policy_uri: str = JOURNAL_POLICY,
    forked_from: Mapping[str, Any] | None = None,
    recorded_at: str | None = None,
    entry_id: str | None = None,
    catalog: ContractCatalog | None = None,
) -> bytes:
    identity = journal_id or new_id()
    require_canonical_uuid_urn(identity)
    policy: dict[str, Any] = {
        "id": identity,
        "serialization": "application/json-seq",
        "entry_encoding": "RFC8785",
        "hash_algorithm": "sha-256",
        "writer_policy": "single_writer",
        "policy_uri": policy_uri,
    }
    if forked_from is not None:
        policy["forked_from"] = dict(forked_from)
    document = _entry(
        journal_id=identity,
        recorder_id=recorded_by_agent_id,
        kind="journal_init",
        body={"journal": policy, "assertions": dict(assertions)},
        recorded_at=recorded_at,
        entry_id=entry_id,
    )
    raw = encode_entry(document, catalog=catalog)
    validate_journal(raw, catalog=catalog)
    return raw


def _append(
    raw: bytes,
    body: Mapping[str, Any],
    *,
    kind: str,
    recorded_by_agent_id: str,
    expected_tail: Mapping[str, Any] | None,
    recorded_at: str | None,
    catalog: ContractCatalog | None,
) -> bytes:
    summary = validate_journal(raw, catalog=catalog)
    if expected_tail is not None and dict(expected_tail) != summary.tail.reference:
        raise ConcurrentJournalChangeError(
            "expected predecessor differs from supplied journal tail"
        )
    document = _entry(
        journal_id=summary.journal_id,
        recorder_id=recorded_by_agent_id,
        kind=kind,
        body=body,
        sequence=len(summary.frames),
        previous=summary.tail.reference,
        recorded_at=recorded_at,
    )
    appended = raw + encode_entry(document, catalog=catalog)
    validate_journal(appended, catalog=catalog)
    return appended


def append_assertions(
    raw: bytes,
    assertions: Mapping[str, Any],
    *,
    recorded_by_agent_id: str,
    expected_tail: Mapping[str, Any] | None = None,
    recorded_at: str | None = None,
    catalog: ContractCatalog | None = None,
) -> bytes:
    return _append(
        raw,
        {"assertions": dict(assertions)},
        kind="assertion",
        recorded_by_agent_id=recorded_by_agent_id,
        expected_tail=expected_tail,
        recorded_at=recorded_at,
        catalog=catalog,
    )


def _assertion_batch_entries(
    raw: bytes,
    batches: Iterable[Mapping[str, Any]],
    *,
    recorded_by_agent_id: str,
    catalog: ContractCatalog | None,
    recorded_at: str | None,
    entry_ids: Iterable[str] | None,
) -> Iterator[bytes]:
    summary = validate_journal(raw, catalog=catalog)
    previous = summary.tail.reference
    sequence = len(summary.frames)
    identities = None if entry_ids is None else iter(entry_ids)
    yield raw
    for assertions in batches:
        document = _entry(
            journal_id=summary.journal_id,
            recorder_id=recorded_by_agent_id,
            kind="assertion",
            body={"assertions": dict(assertions)},
            sequence=sequence,
            previous=previous,
            entry_id=None if identities is None else next(identities),
            recorded_at=recorded_at,
        )
        encoded = encode_entry(document, catalog=catalog)
        yield encoded
        previous = JournalFrame(encoded[1:-1]).reference
        sequence += 1


def write_assertion_batches(
    destination: BinaryIO,
    raw: bytes,
    batches: Iterable[Mapping[str, Any]],
    *,
    recorded_by_agent_id: str,
    catalog: ContractCatalog | None = None,
    recorded_at: str | None = None,
    entry_ids: Iterable[str] | None = None,
) -> JournalSummary:
    """Write bounded entries to an empty seekable spool and validate every prefix.

    The caller publishes only after this returns. Neither recorded bytes nor the
    effective assertion set are collected into one in-memory journal.
    """
    if destination.seek(0, 2) != 0:
        raise ValueError("journal destination must be empty")
    destination.seek(0)
    for encoded in _assertion_batch_entries(
        raw,
        batches,
        recorded_by_agent_id=recorded_by_agent_id,
        catalog=catalog,
        recorded_at=recorded_at,
        entry_ids=entry_ids,
    ):
        if destination.write(encoded) != len(encoded):
            raise OSError("short journal spool write")
    destination.flush()
    destination.seek(0)
    summary = validate_journal_chunks(
        iter(lambda: destination.read(1024 * 1024), b""), catalog=catalog
    )
    destination.seek(0)
    return summary


def append_assertion_batches(
    raw: bytes,
    batches: Iterable[Mapping[str, Any]],
    *,
    recorded_by_agent_id: str,
    catalog: ContractCatalog | None = None,
    recorded_at: str | None = None,
    entry_ids: Iterable[str] | None = None,
) -> bytes:
    """Materialize appended bytes when the caller explicitly wants a bytes value."""
    result = b"".join(
        _assertion_batch_entries(
            raw,
            batches,
            recorded_by_agent_id=recorded_by_agent_id,
            catalog=catalog,
            recorded_at=recorded_at,
            entry_ids=entry_ids,
        )
    )
    validate_journal(result, catalog=catalog)
    return result


def append_correction(
    raw: bytes,
    retracts: Iterable[Mapping[str, Any]],
    *,
    reason: str,
    recorded_by_agent_id: str,
    assertions: Mapping[str, Any] | None = None,
    expected_tail: Mapping[str, Any] | None = None,
    recorded_at: str | None = None,
    catalog: ContractCatalog | None = None,
) -> bytes:
    body: dict[str, Any] = {"reason": reason, "retracts": [dict(item) for item in retracts]}
    if assertions is not None:
        body["assertions"] = dict(assertions)
    return _append(
        raw,
        body,
        kind="correction",
        recorded_by_agent_id=recorded_by_agent_id,
        expected_tail=expected_tail,
        recorded_at=recorded_at,
        catalog=catalog,
    )


def append_checkpoint(
    raw: bytes, *, recorded_by_agent_id: str, purpose: str, catalog: ContractCatalog | None = None
) -> bytes:
    summary = validate_journal(raw, catalog=catalog)
    return _append(
        raw,
        {
            "covered_through": summary.tail.reference,
            "prefix_sha256": summary.journal_sha256,
            "prefix_bytes": str(summary.journal_bytes),
            "purpose": purpose,
        },
        kind="checkpoint",
        recorded_by_agent_id=recorded_by_agent_id,
        expected_tail=summary.tail.reference,
        recorded_at=None,
        catalog=catalog,
    )


def assertion_reference(summary: JournalSummary, assertion_id: str) -> dict[str, Any]:
    return {"entry": summary.assertion_entries[assertion_id], "assertion_id": assertion_id}


def external_reference(summary: JournalSummary, object_id: str) -> dict[str, Any]:
    effective = summary.graph_validation.objects.get(object_id)
    if effective is None:
        raise KeyError(object_id)
    origin = assertion_reference(summary, effective["assertion_id"])
    return {
        "scope": "external",
        "journal_id": summary.journal_id,
        **origin,
        "object_id": object_id,
        "object_type": effective["type"],
    }


@dataclass(frozen=True, slots=True)
class JournalSetValidation:
    journals: tuple[JournalSummary, ...]
    unresolved_references: tuple[dict[str, Any], ...]
    findings: tuple[str, ...]


def validate_journal_set(
    journals: Iterable[bytes],
    *,
    catalog: ContractCatalog | None = None,
    require_all_references: bool = True,
    require_profiles: bool = True,
) -> JournalSetValidation:
    return validate_journal_set_chunks(
        ((journal,) for journal in journals),
        catalog=catalog,
        require_all_references=require_all_references,
        require_profiles=require_profiles,
    )


def validate_journal_set_chunks(
    journals: Iterable[Iterable[bytes]],
    *,
    catalog: ContractCatalog | None = None,
    require_all_references: bool = True,
    require_profiles: bool = True,
) -> JournalSetValidation:
    """Resolve exact foreign assertions; do not silently import effective graphs."""
    summaries: dict[str, JournalSummary] = {}
    for chunks in journals:
        journal = validate_journal_chunks(
            chunks, catalog=catalog, require_profiles=require_profiles
        )
        old = summaries.get(journal.journal_id)
        if old:
            short, long = sorted((old, journal), key=lambda item: len(item.frames))
            if short.frames != long.frames[: len(short.frames)]:
                raise ProvenanceValidationError(
                    "divergent writers reused the same journal identity"
                )
            summaries[journal.journal_id] = long
        else:
            summaries[journal.journal_id] = journal
    # Identity immutability applies to the full supplied documentary history,
    # including retired statements, not only its effective graph.
    assertion_history: dict[str, bytes] = {}
    referent_history: dict[str, bytes] = {}
    entry_history: dict[str, bytes] = {}
    for summary in summaries.values():
        for frame in summary.frames:
            entry_id = frame.document["id"]
            old_entry = entry_history.setdefault(entry_id, frame.json_bytes)
            if old_entry != frame.json_bytes:
                raise ProvenanceValidationError("entry identity reused across journals")
            for _, row in iter_assertions(_body_assertions(frame.document)):
                value = canonical_document(row)
                prior = assertion_history.setdefault(row["assertion_id"], value)
                if prior != value:
                    raise ProvenanceValidationError("assertion identity redefined across journals")
                signature = _identity_signature(row)
                previous = referent_history.setdefault(row["id"], signature)
                if previous != signature:
                    raise ProvenanceValidationError(
                        "immutable referent redefined across documentary histories"
                    )
    domains = [set(summaries), set(entry_history), set(assertion_history), set(referent_history)]
    if any(left & right for i, left in enumerate(domains) for right in domains[i + 1 :]):
        raise ProvenanceValidationError(
            "journal/entry/assertion/referent identity domains overlap across journals"
        )
    unresolved: list[dict[str, Any]] = []
    findings: list[str] = []
    resolved: dict[bytes, dict[str, Any]] = {}
    derivations: list[tuple[str, str]] = []
    specializations: list[tuple[str, str]] = []
    signatures: dict[str, bytes] = {}
    for summary in summaries.values():
        for row in summary.graph_validation.objects.values():
            signature = _identity_signature(row)
            if row["id"] in signatures and signatures[row["id"]] != signature:
                raise ProvenanceValidationError(
                    "conflicting immutable entity definitions across journals"
                )
            signatures[row["id"]] = signature
            if row["type"] == "derivation":
                derivations.append(
                    (row["used_state"]["object_id"], row["generated_state"]["object_id"])
                )
            elif row["type"] == "specialization":
                specializations.append((row["specific"]["object_id"], row["general"]["object_id"]))
        for reference in summary.graph_validation.external_references:
            target = summaries.get(reference["journal_id"])
            if target is None:
                unresolved.append(reference)
                continue
            matches = [f for f in target.frames if f.reference == reference["entry"]]
            if len(matches) != 1:
                raise ProvenanceValidationError("foreign entry anchor is not present exactly")
            rows = [
                row
                for _, row in iter_assertions(_body_assertions(matches[0].document))
                if row["assertion_id"] == reference["assertion_id"]
            ]
            if (
                len(rows) != 1
                or rows[0]["id"] != reference["object_id"]
                or rows[0]["type"] != reference["object_type"]
            ):
                raise ProvenanceValidationError(
                    "foreign reference does not match the anchored assertion"
                )
            resolved[canonical_document(reference)] = rows[0]
            if reference["assertion_id"] in target.retracted_assertion_ids:
                findings.append(
                    f"{reference['assertion_id']}: referenced historical assertion is retracted "
                    "at supplied foreign tail"
                )
        parent = summary.frames[0].document["body"]["journal"].get("forked_from")
        if parent:
            target = summaries.get(parent["journal_id"])
            if target is None:
                if require_all_references:
                    raise ProvenanceValidationError("fork prefix anchor could not be resolved")
                findings.append("unresolved journal fork prefix anchor")
            else:
                validate_journal_chunks(
                    (frame.encoded for frame in target.frames),
                    catalog=catalog,
                    expected_anchor=parent,
                    require_profiles=require_profiles,
                )
    if unresolved and require_all_references:
        raise ProvenanceValidationError("unresolved external journal references")
    for summary in summaries.values():
        objects = summary.graph_validation.objects
        for row in objects.values():
            if row["type"] != "content_comparison":
                continue
            descriptions = []
            for key in ("left_description", "right_description"):
                ref = row[key]
                descriptions.append(
                    objects.get(ref["object_id"])
                    if ref["scope"] == "local"
                    else resolved.get(canonical_document(ref))
                )
            if any(item is None for item in descriptions):
                continue
            if any(item is None or "content" not in item for item in descriptions):
                if row["result"] != "indeterminate":
                    raise ProvenanceValidationError(
                        "resolved comparison lacks complete cited fixity"
                    )
                continue
            left_description, right_description = descriptions
            assert left_description is not None and right_description is not None
            same = fingerprint(left_description["content"]) == fingerprint(
                right_description["content"]
            )
            if (row["result"] == "matching_fixity" and not same) or (
                row["result"] == "different" and same
            ):
                raise ProvenanceValidationError(
                    "comparison contradicts resolved foreign content evidence"
                )
    _acyclic(derivations, "cross-journal derivation")
    _acyclic(specializations, "cross-journal specialization")
    return JournalSetValidation(tuple(summaries.values()), tuple(unresolved), tuple(findings))


@dataclass(frozen=True, slots=True)
class RecoveryResult:
    complete_prefix: bytes
    incomplete_tail: bytes
    prefix_validation: JournalSummary


def recover_complete_prefix(
    raw: bytes, *, catalog: ContractCatalog | None = None
) -> RecoveryResult:
    """Explicitly return an untrusted torn tail; never mark the whole input valid."""
    last_lf = raw.rfind(LF)
    if last_lf < 0:
        raise ProvenanceValidationError("no complete journal prefix")
    prefix, tail = raw[: last_lf + 1], raw[last_lf + 1 :]
    summary = validate_journal(prefix, catalog=catalog)
    if tail and (not tail.startswith(RS) or RS in tail[1:]):
        raise ProvenanceValidationError("tail is not one incomplete RFC 7464 frame")
    if len(tail) > PROVENANCE_JOURNAL_ENTRY_BYTES_MAX + 1:
        raise ProvenanceValidationError("incomplete tail exceeds the frame limit")
    return RecoveryResult(prefix, tail, summary)


def append_observation(
    raw: bytes,
    result: Any,
    *,
    expected_tail: Mapping[str, Any] | None = None,
    catalog: ContractCatalog | None = None,
) -> bytes:
    """Append a result, omitting only identical already-declared identity objects.

    New observations, activities, bindings and attributed evidence are not
    coalesced merely because their payload digests happen to match.
    """
    from .model import ObservationResult

    if not isinstance(result, ObservationResult):
        raise TypeError("result must be an ObservationResult")
    summary = validate_journal(raw, catalog=catalog)
    existing = summary.graph_validation.objects
    retained: list[tuple[str, dict[str, Any]]] = []
    for category, row in iter_assertions(result.graph_fragment()):
        old = existing.get(row["id"])
        if old is None:
            retained.append((category, row))
        elif row["type"] in IDENTITY_TYPES:
            left = {k: v for k, v in row.items() if k != "assertion_id"}
            right = {k: v for k, v in old.items() if k != "assertion_id"}
            if left != right:
                raise ProvenanceValidationError(
                    "identity descriptions differ; an explicit correction or new identity "
                    "is required"
                )
        else:
            raise ProvenanceValidationError(
                "observation result reuses an existing non-identity object"
            )
    return append_assertions(
        raw,
        graph_from_assertions(retained),
        recorded_by_agent_id=result.observer_agent_id,
        expected_tail=expected_tail,
        catalog=catalog,
    )
