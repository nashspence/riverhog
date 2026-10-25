"""Pure append-only RFC 7464 journal operations, independent of payload storage.

A caller owns durable writes and writer arbitration. These functions return
validated bytes; no filename, global current state, database or filesystem is
assumed. Exact existing prefixes are never reserialized.
"""

from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Iterable, Iterator, Mapping, Sequence, Set
from contextlib import ExitStack
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any, BinaryIO, Self, cast

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)
from riverhog_provenance_contracts import (
    ENTRY_SCHEMA,
    PROFILE,
    ContractCatalog,
    ExternalReference,
    canonical_document,
    decode_document,
    require_canonical_uuid_urn,
)
from sha256_continuation import CheckpointSHA256, sha256_compress

from .common import fingerprint, new_id, utc_now
from .constants import JOURNAL_POLICY, PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
from .errors import ConcurrentJournalChangeError, ProvenanceValidationError
from .graph import (
    IDENTITY_TYPES,
    GraphValidation,
    _acyclic,
    graph_from_assertions,
    iter_assertions,
)

if TYPE_CHECKING:
    from .journal_store import JournalStore

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


def _source_anchor_declaration() -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    return Declaration(
        id="riverhog-provenance.frozen-source-anchor.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance.journal.JournalSummary.anchor_at",),
        scope="frozen-native-source-prefix-header-selection",
        starts_at="select an accepted frame sequence from an owned frozen source summary",
        ends_at="return its exact flat SHA-256 prefix anchor after closing metadata cursors",
        dimensions=(
            Dimension("retained_frames", COUNT, "Accepted frames in the frozen source."),
            Dimension("sequence_octets", OCTETS, "Full UTF-8 decimal lookup ordinal width."),
            Dimension(
                "header_octets", OCTETS, "Complete indexed reference and SHA continuation octets."
            ),
            Dimension(
                "padding_blocks", COUNT, "One for a source SHA tail below 56 octets; otherwise two."
            ),
        ),
        bounds=(
            Bound("sql_statements", COUNT, "equal", count(1), "One primary-key header selection."),
            Bound(
                "returned_rows",
                COUNT,
                "equal",
                count(1),
                "Exactly the selected accepted source header.",
            ),
            Bound(
                "closed_cursors",
                COUNT,
                "equal",
                count(1),
                "Metadata cursors close before digest finalization.",
            ),
            Bound(
                "bound_parameter_octets",
                OCTETS,
                "equal",
                Expression("dimension", name="sequence_octets"),
                "The complete source ordinal is bound once.",
            ),
            Bound(
                "metadata_octets",
                OCTETS,
                "equal",
                Expression("dimension", name="header_octets"),
                "Both indexed header fields are read completely.",
            ),
            Bound(
                "json_decodes",
                COUNT,
                "equal",
                count(2),
                "The exact reference and trusted SHA codec.",
            ),
            Bound(
                "json_encodes",
                COUNT,
                "equal",
                count(2),
                "Verify each owning canonical header codec.",
            ),
            Bound(
                "decoded_octets",
                OCTETS,
                "equal",
                Expression("dimension", name="header_octets"),
                "Complete header codec inputs, never frame payload.",
            ),
            Bound(
                "encoded_octets",
                OCTETS,
                "equal",
                Expression("dimension", name="header_octets"),
                "Round trips reproduce the complete canonical header bytes.",
            ),
            Bound(
                "source_read_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "No journal frame or source range is read.",
            ),
            Bound(
                "source_update_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "No source update is supplied to the SHA continuation.",
            ),
            Bound(
                "compression_calls",
                COUNT,
                "equal",
                Expression("dimension", name="padding_blocks"),
                "Finalize a copy of the selected exact prefix continuation.",
            ),
            *tuple(
                Bound(
                    "padding_" + metric,
                    unit,
                    "equal",
                    Expression(
                        "delegate",
                        name="padding",
                        metric=metric,
                        terms=(Expression("dimension", name="padding_blocks"),),
                        arguments=(
                            ("block_octets", Expression("constant", value=64, unit=OCTETS)),
                        ),
                    ),
                    "Actual mandatory native SHA padding costs.",
                )
                for metric, unit in (
                    ("block_input_octets", OCTETS),
                    ("rotate_calls", COUNT),
                    ("returned_words", COUNT),
                )
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        count(512),
                        Expression(
                            "product",
                            terms=(
                                count(128),
                                Expression(
                                    "log2_ceil",
                                    terms=(
                                        Expression(
                                            "sum",
                                            terms=(
                                                Expression("dimension", name="retained_frames"),
                                                count(2),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "Indexed metadata lookup growth under the maintained native SQLite engine.",
            ),
        ),
        conditions=(
            "The source summary owns an open frozen native projection of a fully accepted journal.",
            "Header and source progress commit together under the native validation fence.",
            "Padding count derives from the exact input continuation before the call.",
            "The caller retains source admission and current access policy.",
        ),
        invariants=(
            "Return the original flat journal-prefix SHA-256, bytes and exact entry reference.",
            "No historical semantic view or current-tail substitution is returned.",
            "The selected prefix and owning frozen summary remain unchanged.",
        ),
        limitations=(
            "Validation, custody, authorization and historical fact traversal have separate costs.",
            "Codec and padding inputs have the existing bounded native header shapes.",
            "VM constants depend on SQLite; elapsed time, I/O and lock waits remain observations.",
        ),
    )


def _source_reference_declaration() -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    return Declaration(
        id="riverhog-provenance.frozen-source-reference.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance.journal.JournalSummary.reference_anchor",),
        scope="frozen-native-exact-documentary-reference-selection",
        starts_at="receive one complete external reference and an owned accepted frozen source",
        ends_at="return its exact original prefix anchor without loading assertion or frame bodies",
        dimensions=(
            Dimension("indexed_assertions", COUNT, "All accepted active and retired assertions."),
            Dimension("assertion_key_octets", OCTETS, "The complete canonical assertion UUID URN."),
            Dimension("metadata_octets", OCTETS, "Complete sequence, object UUID and type fields."),
        ),
        bounds=(
            Bound(
                "sql_statements", COUNT, "equal", count(1), "One exact assertion primary-key seek."
            ),
            Bound("returned_rows", COUNT, "equal", count(1), "One original documentary address."),
            Bound("closed_cursors", COUNT, "equal", count(1), "Close before the anchor delegate."),
            Bound(
                "bound_parameter_octets",
                OCTETS,
                "equal",
                Expression("dimension", name="assertion_key_octets"),
                "The full assertion identity is bound once.",
            ),
            Bound(
                "metadata_octets",
                OCTETS,
                "equal",
                Expression("dimension", name="metadata_octets"),
                "No assertion payload or effective-object projection is fetched.",
            ),
            Bound(
                "native_anchor_calls",
                COUNT,
                "equal",
                count(1),
                "The existing source-header delegate supplies the exact original prefix.",
            ),
            Bound(
                "source_read_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "Neither the reference lookup nor its delegate reads journal payload.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "product",
                    terms=(
                        count(256),
                        Expression(
                            "log2_ceil",
                            terms=(
                                Expression(
                                    "sum",
                                    terms=(
                                        Expression("dimension", name="indexed_assertions"),
                                        count(2),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "One maintained SQLite B-tree point read, excluding the anchor delegate.",
            ),
        ),
        conditions=(
            "The source owns an open frozen independently validated Native projection.",
            "The caller retains exact archive/source admission and current access checks.",
            "The external reference has the existing flat canonical Native schema.",
        ),
        invariants=(
            "Journal, assertion, object, type and every original entry coordinate match exactly.",
            "Retired documentary assertions remain addressable without effective attribution.",
            "The exact original prefix is returned, never the current tail or another referent.",
            "All lookup cursors close before the Native source-anchor delegate executes.",
        ),
        limitations=(
            "Direct metrics exclude the separately witnessed anchor delegate and its padding.",
            "Reference schema validation uses existing bounded identity/header field shapes.",
            "This supplies documentary address proof, not corpus or effective-history validity.",
            "Source validation, custody, current authority and filesystem lifetime are separate.",
            "VM constants depend on SQLite; I/O, lock waits and elapsed time are observations.",
        ),
    )


def _source_fork_declaration() -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    return Declaration(
        id="riverhog-provenance.frozen-source-fork.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance.journal.JournalSummary.fork_anchor",),
        scope="one authenticated initialization atom from a frozen Native journal",
        starts_at="receive an open independently validated frozen source summary",
        ends_at="return its detached recorded fork anchor or explicit absence",
        dimensions=(
            Dimension("retained_frames", COUNT, "All accepted source frames."),
            Dimension("initial_octets", OCTETS, "Complete canonical initialization JSON bytes."),
        ),
        bounds=(
            Bound("sql_statements", COUNT, "equal", count(1), "One indexed initial-frame read."),
            Bound("returned_rows", COUNT, "equal", count(1), "The original initialization atom."),
            Bound("closed_cursors", COUNT, "equal", count(1), "Close before decoding the atom."),
            Bound(
                "bound_parameter_octets",
                OCTETS,
                "equal",
                Expression("constant", value=1, unit=OCTETS),
                "The exact sequence zero key.",
            ),
            *(
                Bound(
                    metric,
                    OCTETS,
                    "equal",
                    Expression("dimension", name="initial_octets"),
                    "Read and decode the complete initialization atom exactly once.",
                )
                for metric in ("source_read_octets", "decoded_octets", "encoded_octets")
            ),
            Bound("json_decodes", COUNT, "equal", count(1), "One original document decode."),
            Bound("json_encodes", COUNT, "equal", count(1), "Verify the original canonical bytes."),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        count(512),
                        Expression(
                            "product",
                            terms=(
                                count(128),
                                Expression(
                                    "log2_ceil",
                                    terms=(
                                        Expression(
                                            "sum",
                                            terms=(
                                                Expression("dimension", name="retained_frames"),
                                                count(2),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "One maintained B-tree point read; no journal traversal.",
            ),
        ),
        conditions=(
            "The source owns an open frozen independently validated Native projection.",
            "The initialization atom remains subject to Native's entry admission limit.",
            "The caller retains exact source admission, custody and current authority.",
        ),
        invariants=(
            "The recorded fork comes from the original initialization entry only.",
            "Source tail length never causes prefix replay or another frame read.",
            "Absence is explicit, and the returned mapping cannot mutate the source.",
        ),
        limitations=(
            "Decoding costs the complete initialization atom, including unrelated assertions.",
            "Calling once per qualified journal is the closure owner's separate obligation.",
            "A recorded fork is not proof that its foreign source is present or accepted.",
            "SQL VM constants depend on SQLite; I/O and elapsed time remain observations.",
        ),
    )


@dataclass(frozen=True, slots=True)
class JournalSummary:
    journal_id: str
    frames: Sequence[JournalFrame]
    graph_validation: GraphValidation
    retracted_assertion_ids: Set[str]
    journal_sha256: str
    journal_bytes: int
    assertion_entries: Mapping[str, dict[str, str]]
    _store: JournalStore | None = field(default=None, init=False, repr=False, compare=False)

    def close(self) -> None:
        """Release this owned handle; its views borrow the same handle's lifetime."""
        if self._store is not None:
            self._store.close()

    def __enter__(self) -> Self:
        if self._store is not None:
            self._store.require_open()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    @declare_growth(
        Declaration(
            id="riverhog-provenance.frozen-summary-borrow.growth/v1",
            authority="riverhog-provenance",
            subjects=("riverhog_provenance.journal.JournalSummary.borrow",),
            scope="frozen-snapshot-reader-lease",
            starts_at="borrow an open independently validated frozen summary",
            ends_at="return one independently closeable handle sharing its exact projection",
            dimensions=(
                Dimension("snapshot_octets", OCTETS, "Exact octets validated by the snapshot."),
            ),
            bounds=(
                Bound(
                    "returned_handles",
                    COUNT,
                    "equal",
                    Expression("constant", value=1, unit=COUNT),
                    "One new independently closeable summary is returned.",
                ),
                Bound(
                    "sql_statements",
                    COUNT,
                    "equal",
                    Expression("constant", value=0, unit=COUNT),
                    "Borrowing performs no SQLite statement or snapshot scan.",
                ),
                Bound(
                    "opened_databases",
                    COUNT,
                    "equal",
                    Expression("constant", value=0, unit=COUNT),
                    "Borrowing retains the existing frozen projection instead of opening a copy.",
                ),
            ),
            conditions=("The input owns an open frozen native journal projection.",),
            invariants=(
                "Exact journal bytes, commitments and semantic facts are shared unchanged.",
                "Closing this lease cannot invalidate a different live owner's lease.",
            ),
            limitations=(
                "This primitive profile excludes validation, caller traversal and materialization.",
                "Final-owner disposal and filesystem work require separate accounting; "
                "cleanup is not claimed constant-time.",
                "Lock wait and physical CPU time are observations, not primitive statement counts.",
            ),
        )
    )
    def borrow(self) -> JournalSummary:
        """Return an independently closeable handle to this exact frozen snapshot.

        A reader may close its handle without invalidating another owner. Iterators
        borrow their handle; closing an iterator never closes the snapshot.
        """
        if self._store is None:
            return replace(self)
        store = self._store.borrow()
        try:
            summary = replace(
                self,
                frames=store.frames,
                graph_validation=store.graph_validation,
                retracted_assertion_ids=store.retired,
                assertion_entries=store.origins,
            )
            object.__setattr__(summary, "_store", store)
            return summary
        except BaseException:
            store.close()
            raise

    @declare_growth(_source_anchor_declaration(), delegates=(("padding", sha256_compress),))
    def anchor_at(self, sequence: int) -> dict[str, Any]:
        """Return the exact source prefix at an accepted frame without replay.

        This source header does not provide an effective historical graph or
        replace source admission, custody or the caller's current permissions.
        The summary must own an open frozen native projection.
        """
        from ._journal_prefix import _reference
        from .journal_store import InvalidJournalWorkspace

        if type(sequence) is not int:
            raise TypeError("source frame sequence must be an integer")
        if self._store is None:
            raise RuntimeError("source headers require an owned native journal summary")
        self._store.require_open()
        if not 0 <= sequence < len(self.frames):
            raise IndexError(sequence)
        encoded, state = self._store._frame_prefix(sequence)
        try:
            reference = _reference(json.loads(encoded))
            digest = CheckpointSHA256.from_state(state)
            if (
                reference["sequence"] != str(sequence)
                or canonical_document(reference) != encoded
                or not 0 < digest.total_bytes <= self.journal_bytes
            ):
                raise ValueError("source prefix header differs from its frozen source")
        except (ValueError, TypeError, UnicodeError) as error:
            raise InvalidJournalWorkspace("accepted journal source header is corrupt") from error
        return {
            "journal_id": self.journal_id,
            "through": reference,
            "prefix_sha256": digest.hexdigest(),
            "prefix_bytes": str(digest.total_bytes),
        }

    @declare_growth(_source_fork_declaration())
    def fork_anchor(self) -> dict[str, Any] | None:
        """Read the recorded fork from exactly one original initialization atom.

        Native validation already owns this metadata's grammar. The caller must
        independently resolve and admit any returned foreign prefix.
        """
        from .journal_store import InvalidJournalWorkspace

        if self._store is None:
            raise RuntimeError("fork metadata requires an owned native journal summary")
        raw = self._store._initial_frame()
        document = decode_document(raw)
        if (
            document["journal_id"] != self.journal_id
            or document["sequence"] != "0"
            or document["entry_kind"] != "journal_init"
        ):
            raise InvalidJournalWorkspace("accepted journal initialization coordinates changed")
        # Decoding detaches this complete atom from its frozen source. Return
        # only the Native-defined metadata, never the initial assertion graph.
        return cast(dict[str, Any] | None, document["body"]["journal"].get("forked_from"))

    @declare_growth(_source_reference_declaration(), delegates=(("anchor", anchor_at),))
    def reference_anchor(self, reference: Mapping[str, Any]) -> dict[str, Any]:
        """Resolve an exact documentary assertion without replaying its source.

        Retraction does not erase a documentary address. This method does not
        attribute the assertion to an effective historical graph, accept a
        corpus, or replace the caller's source admission/current access checks.
        """
        from .journal_store import InvalidJournalWorkspace

        if self._store is None:
            raise RuntimeError("source references require an owned native journal summary")
        self._store.require_open()
        selected = ExternalReference.model_validate(reference).model_dump(mode="json")
        if selected["journal_id"] != self.journal_id:
            raise ProvenanceValidationError("foreign reference names another source journal")
        original = self._store._assertion_origin(selected["assertion_id"])
        if original is None:
            raise ProvenanceValidationError("required foreign entry or assertion is absent")
        sequence, object_id, kind = original
        try:
            ordinal = int(sequence)
            if str(ordinal) != sequence or not 0 <= ordinal < len(self.frames):
                raise ValueError("invalid original source position")
        except (ValueError, TypeError) as error:
            raise InvalidJournalWorkspace(
                "accepted assertion source position is corrupt"
            ) from error
        anchor = self.anchor_at(ordinal)
        if (
            anchor["through"] != selected["entry"]
            or object_id != selected["object_id"]
            or kind != selected["object_type"]
        ):
            raise ProvenanceValidationError("foreign reference differs from its exact assertion")
        return anchor

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
            "through": (
                self._store.frame_reference(-1) if self._store is not None else self.tail.reference
            ),
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
    from uuid import uuid4

    from .journal_session import JournalValidationSession

    with JournalValidationSession(
        chunks,
        attempt_id=uuid4().hex,
        catalog=catalog,
        require_profiles=require_profiles,
        expected_anchor=expected_anchor,
        require_exact_tail=require_exact_tail,
    ) as session:
        while not session.advance().complete:
            pass
        return session.result()


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
    validate_journal(raw, catalog=catalog).close()
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
    with validate_journal(raw, catalog=catalog) as summary:
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
        validate_journal(appended, catalog=catalog).close()
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
    with validate_journal(raw, catalog=catalog) as summary:
        previous = summary.tail.reference
        sequence = len(summary.frames)
        journal_id = summary.journal_id
    identities = None if entry_ids is None else iter(entry_ids)
    yield raw
    for assertions in batches:
        document = _entry(
            journal_id=journal_id,
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
    try:
        destination.seek(0)
    except BaseException:
        summary.close()
        raise
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
    validate_journal(result, catalog=catalog).close()
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
    with validate_journal(raw, catalog=catalog) as summary:
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

    def close(self) -> None:
        for journal in self.journals:
            journal.close()

    def __enter__(self) -> Self:
        try:
            for journal in self.journals:
                journal.__enter__()
        except BaseException:
            self.close()
            raise
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


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
    with ExitStack() as owners:
        summaries: dict[str, JournalSummary] = {}
        for chunks in journals:
            journal = validate_journal_chunks(
                chunks, catalog=catalog, require_profiles=require_profiles
            )
            owners.enter_context(journal)
            old = summaries.get(journal.journal_id)
            if old:
                short, long = sorted((old, journal), key=lambda item: len(item.frames))
                if short.frames != long.frames[: len(short.frames)]:
                    raise ProvenanceValidationError(
                        "divergent writers reused the same journal identity"
                    )
                short.close()
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
                        raise ProvenanceValidationError(
                            "assertion identity redefined across journals"
                        )
                    signature = _identity_signature(row)
                    previous = referent_history.setdefault(row["id"], signature)
                    if previous != signature:
                        raise ProvenanceValidationError(
                            "immutable referent redefined across documentary histories"
                        )
        domains = [
            set(summaries),
            set(entry_history),
            set(assertion_history),
            set(referent_history),
        ]
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
                    specializations.append(
                        (row["specific"]["object_id"], row["general"]["object_id"])
                    )
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
                        f"{reference['assertion_id']}: referenced historical assertion "
                        "is retracted "
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
                    ).close()
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
        result = JournalSetValidation(tuple(summaries.values()), tuple(unresolved), tuple(findings))
        owners.pop_all()
        return result


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
    try:
        if tail and (not tail.startswith(RS) or RS in tail[1:]):
            raise ProvenanceValidationError("tail is not one incomplete RFC 7464 frame")
        if len(tail) > PROVENANCE_JOURNAL_ENTRY_BYTES_MAX + 1:
            raise ProvenanceValidationError("incomplete tail exceeds the frame limit")
        return RecoveryResult(prefix, tail, summary)

    except BaseException:
        summary.close()
        raise


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
    with validate_journal(raw, catalog=catalog) as summary:
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
