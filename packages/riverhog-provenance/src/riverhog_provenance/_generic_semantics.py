"""Private, source-fenced continuation for common assertion checks.

The admitted row is one existing entry-bounded atom. Its immutable container index
is prepared once outside transactions. Variable traversal frames and duplicate
metadata keys live in indexed rows. Semantic outputs and progress commit together;
an invalid row quarantines this attempt without publishing an accepted prefix.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections.abc import Iterator, Mapping, MutableMapping, MutableSet
from contextlib import closing
from dataclasses import dataclass, replace
from typing import Any

from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    OCTETS_PER_COUNT,
    SCALAR,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)
from riverhog_provenance_contracts import (
    ContractCatalog,
    canonical_document,
    require_canonical_uuid_urn,
)

from ._activity_projection import read_activity
from ._binding_projection import read_binding_time
from ._delivery_projection import read_delivery_metadata, read_locator_observation
from ._description_projection import read_description_metadata
from ._fixity_projection import read_fixity_metadata
from ._index_key import encode_index_key
from ._journal_prefix import PrefixState
from ._observation_coverage import ObservationCoverage
from ._observation_projection import has_observer, read_observation_metadata
from ._relation_projection import read_relation_metadata
from ._semantic_edge import EdgeAdmission, EdgeBatch, SemanticEdge
from ._semantic_program import ROW_STAGES, ROW_TYPES, Frame, MemoryStack, Nodes, Program, Stack
from ._semantic_source import SemanticSource
from .constants import PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
from .graph import _Validator

ALGORITHM = "riverhog-generic-semantics/v11"
MAX_STEPS = 64
_HEADER_OCTETS = 2048
_BINDING_OCTETS = 8192
_FRAME_OCTETS = 512
# Fixed JSON keys, one 45-octet UUID and an optional 30-octet native UTC timestamp.
_LIFECYCLE_OCTETS = 128
_DEFAULT_BYTES = 8 * PROVENANCE_JOURNAL_ENTRY_BYTES_MAX + 65536
_RESULT_BYTES = 8 * PROVENANCE_JOURNAL_ENTRY_BYTES_MAX + 8192
_STAGES = {
    "row": max(ROW_STAGES.values()),
    "association": 1,
    "activity_context": 1,
    "generic": 6,
    "evidence": 3,
    "identifier": 2,
    "source": 2,
    "profile": 1,
    "filesystem": 6,
    "principal": 3,
    "stamp": 4,
    "native": 5,
    "interpretation": 2,
    "typed": 1,
    "content": 1,
}


class _GraphRequest(Exception):
    """The next native row transition requires an owned graph child."""


class StaleGenericSemantics(RuntimeError):
    """The source, semantic subject, prefix, worker or proposal was replaced."""


class GenericSemanticsCapacity(RuntimeError):
    """The next existing atom requires more byte admission."""


@dataclass(frozen=True)
class GenericAtom:
    encoded: bytes
    sha256: str
    nodes: Nodes

    @classmethod
    def admit(cls, row: Mapping[str, Any]) -> GenericAtom:
        require_canonical_uuid_urn(row["assertion_id"])
        require_canonical_uuid_urn(row["id"])
        encoded = canonical_document(row)
        if len(encoded) > PROVENANCE_JOURNAL_ENTRY_BYTES_MAX:
            raise ValueError("semantic row exceeds its owning entry envelope")
        # The caller cannot mutate the admitted tree through its original row.
        return cls(encoded, hashlib.sha256(encoded).hexdigest(), Nodes(json.loads(encoded)))


@dataclass(frozen=True)
class GenericState:
    phase: str = "walk"
    top: int | None = 0
    serial: int = 1
    version: int = 0
    error_sha256: str | None = None
    failure: str | None = None
    edge_run: str | None = None

    @property
    def done(self) -> bool:
        return self.phase in {"done", "rejected"}

    def to_json(self) -> str:
        return canonical_json_bytes(
            {
                "phase": self.phase,
                "top": None if self.top is None else str(self.top),
                "serial": str(self.serial),
                "version": str(self.version),
                "error_sha256": self.error_sha256,
                "failure": self.failure,
                "edge_run": self.edge_run,
            }
        ).decode()

    @classmethod
    def from_json(cls, raw: str) -> GenericState:
        try:
            if type(raw) is not str or len(raw) > _HEADER_OCTETS:
                raise ValueError("checkpoint envelope")
            value = json.loads(raw)
            if (
                type(value) is not dict
                or set(value)
                != {"phase", "top", "serial", "version", "error_sha256", "failure", "edge_run"}
                or value["phase"]
                not in {
                    "walk",
                    "edge",
                    "keys",
                    "outputs",
                    "reject-frames",
                    "reject-keys",
                    "reject-outputs",
                    "done",
                    "rejected",
                }
            ):
                raise ValueError("checkpoint shape")
            for key in ("serial", "version", "top"):
                item = value[key]
                if key == "top" and item is None:
                    continue
                if type(item) is not str or re.fullmatch(r"0|[1-9][0-9]*", item) is None:
                    raise ValueError("checkpoint cursor")
            state = cls(
                value["phase"],
                None if value["top"] is None else int(value["top"]),
                int(value["serial"]),
                int(value["version"]),
                value["error_sha256"],
                value["failure"],
                value["edge_run"],
            )
            if state.serial < 1 or state.serial > 64 * PROVENANCE_JOURNAL_ENTRY_BYTES_MAX:
                raise ValueError("entry-local frame allocation")
            if state.top is not None and not 0 <= state.top < state.serial:
                raise ValueError("stack cursor")
            if (state.phase in {"walk", "edge"}) != (state.top is not None):
                raise ValueError("stack phase")
            if state.edge_run is not None and (
                type(state.edge_run) is not str
                or not state.edge_run
                or len(state.edge_run.encode()) > _HEADER_OCTETS
                or state.phase
                not in {"edge", "reject-frames", "reject-keys", "reject-outputs", "rejected"}
            ):
                raise ValueError("graph child identity")
            if state.phase == "edge" and state.edge_run is None:
                raise ValueError("missing graph child")
            if state.phase.startswith("reject"):
                if (
                    type(state.error_sha256) is not str
                    or re.fullmatch(r"[0-9a-f]{64}", state.error_sha256) is None
                ):
                    raise ValueError("rejection identity")
                if type(state.failure) is not str or len(state.failure) > _FRAME_OCTETS:
                    raise ValueError("retained failing frame")
            elif state.error_sha256 is not None or state.failure is not None:
                raise ValueError("unexpected rejection")
            if state.to_json() != raw:
                raise ValueError("noncanonical checkpoint")
            return state
        except (ValueError, TypeError, KeyError) as exc:
            raise RuntimeError("corrupt generic semantic checkpoint") from exc


def _encode_frame(frame: Frame) -> str:
    return canonical_json_bytes(
        {
            "operation": frame.operation,
            "node": str(frame.node),
            "stage": str(frame.stage),
            "cursor": str(frame.cursor),
        }
    ).decode()


def _decode_frame(raw: str, nodes: Nodes) -> Frame:
    try:
        if type(raw) is not str or len(raw) > _FRAME_OCTETS:
            raise ValueError("frame envelope")
        value = json.loads(raw)
        if type(value) is not dict or set(value) != {"operation", "node", "stage", "cursor"}:
            raise ValueError("frame shape")
        for key in ("node", "stage", "cursor"):
            if type(value[key]) is not str or re.fullmatch(r"0|[1-9][0-9]*", value[key]) is None:
                raise ValueError("frame cursor")
        frame = Frame(
            value["operation"], int(value["node"]), int(value["stage"]), int(value["cursor"])
        )
        if (
            frame.operation not in _STAGES
            or not 0 <= frame.stage <= _STAGES[frame.operation]
            or not 0 <= frame.node < len(nodes.values)
            or not 0 <= frame.cursor <= PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
        ):
            raise ValueError("frame identity")
        if _encode_frame(frame) != raw:
            raise ValueError("noncanonical frame")
        source = nodes.values[frame.node]
        if type(source) is not dict:
            raise ValueError("semantic frame does not identify an object")
        if frame.operation == "content" and type(source.get("digests")) is not list:
            raise ValueError("semantic content frame does not identify its collection")
        if frame.operation == "row" and (
            source.get("type") not in ROW_TYPES
            or frame.stage > ROW_STAGES[source["type"]]
            or frame.node != 0
        ):
            raise ValueError("semantic row frame differs from its admitted subject")
        arrays = {
            "generic": {0: "evidence", 1: "identifiers", 2: "profiles", 3: "metadata"},
            "filesystem": {
                0: "identifiers",
                1: "native_identifiers",
                2: "principals",
                3: "timestamps",
            },
            "native": {4: "interpretations"},
            "content": {0: "digests"},
        }
        array = arrays.get(frame.operation, {}).get(frame.stage)
        if frame.operation == "row" and source["type"] == "activity":
            array = {3: "associations", 4: "contexts", 5: "details"}.get(frame.stage)
        elif frame.operation == "row" and source["type"] in {"reported_description", "observation"}:
            array = {4: "coverage", 5: "profiles", 12: "evidence", 16: "metadata"}.get(frame.stage)
        maximum = (
            len(source.get(array, []))
            if array is not None
            else 4
            if frame.operation == "filesystem" and frame.stage == 5
            else 0
        )
        if frame.cursor > maximum:
            raise ValueError("semantic cursor differs from its source collection")
        return frame
    except (ValueError, TypeError, KeyError) as exc:
        raise RuntimeError("corrupt generic semantic frame") from exc


def _error_identity(error: ValueError) -> str:
    return hashlib.sha256(
        canonical_json_bytes(
            {"type": type(error).__module__ + "." + type(error).__qualname__, "message": str(error)}
        )
    ).hexdigest()


def _declaration() -> Declaration:
    budget = Expression("dimension", name="step_budget")

    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def linear(fixed: int, multiplier: int) -> Expression:
        return Expression(
            "sum",
            terms=(
                count(fixed),
                Expression(
                    "product",
                    terms=(
                        Expression("constant", value=multiplier, unit=SCALAR),
                        budget,
                    ),
                ),
            ),
        )

    statements, returned = linear(64, 16), linear(32, 4)
    width = Expression("dimension", name="metadata_row_octets")
    return Declaration(
        id="riverhog-provenance.sqlite-generic-semantics.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance._generic_semantics.GenericSemantics.advance",),
        scope="native-sqlite-source-fenced-common-semantic-unit",
        starts_at="load the exact admitted row, source, accepted prefix and semantic cursor",
        ends_at="commit bounded checks or rejection undo/reclamation with their checkpoint",
        dimensions=(
            Dimension("retained_rows", COUNT, "Rows retained in the owned private workspace."),
            Dimension("step_budget", COUNT, "One to 64 control/reference or cleanup steps."),
            Dimension(
                "byte_budget",
                OCTETS,
                "Admission for variable key/output atoms and control work.",
            ),
            Dimension(
                "metadata_row_octets",
                OCTETS_PER_COUNT,
                "Actual scalar/control column envelope including identifiers and headers.",
            ),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                statements,
                "Indexed frame/reference/key work, checkpoint fences and output undo.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                returned,
                "Each step visits a fixed number of indexed control/reference records.",
            ),
            Bound(
                "changed_rows",
                COUNT,
                "upper",
                linear(16, 8),
                "Frame/output/undo/key work or one graph child, plus their shared checkpoint.",
            ),
            Bound(
                "returned_metadata_octets",
                OCTETS,
                "upper",
                Expression(
                    "product",
                    terms=(
                        returned,
                        width,
                    ),
                ),
                "Returned columns retain their actual scalar envelope.",
            ),
            Bound(
                "bound_parameter_octets",
                OCTETS,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        Expression(
                            "product",
                            terms=(
                                Expression("constant", value=2, unit=SCALAR),
                                Expression("dimension", name="byte_budget"),
                            ),
                        ),
                        Expression("product", terms=(statements, width)),
                    ),
                ),
                "Admitted key/output arguments and actual control scalar widths.",
            ),
            Bound(
                "adjacency_rows",
                COUNT,
                "upper",
                budget,
                "A graph handoff advances at most one admitted native adjacency window.",
            ),
            Bound(
                "source_payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "Covering identity/type and subject indexes return no assertion/frame payload.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        linear(16384, 6144),
                        Expression(
                            "product",
                            terms=(
                                Expression("constant", value=512, unit=SCALAR),
                                linear(1, 1),
                                Expression(
                                    "log2_ceil",
                                    terms=(
                                        Expression(
                                            "sum",
                                            terms=(
                                                count(2),
                                                Expression("dimension", name="retained_rows"),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "Bounded indexed checks; B-tree byte comparisons stay separate.",
            ),
        ),
        conditions=(
            "The owner retains the exact immutable admitted row/source, sealed catalog and modes.",
            "Covering active-object/subject indexes and indexed variable state are installed.",
            "Staging owns fixed activity, locator-binding, delivery, observation reference fields "
            "and raw observer membership and usage/generation correlates; "
            "lifecycle aggregates retain only "
            "canonical activity identity and optional native timestamp metadata.",
            "The source admission index is constructed once outside checkpoint transactions.",
            "The metadata envelope accounts for actual identifier, cursor and control widths.",
            "Close owner transactions before planning; this phase owns its atomic unit.",
        ),
        invariants=(
            "Native common and supported row check order share one interpreter.",
            "Outputs, graph-child work, undo ownership and progress share "
            "the exact source/prefix fence.",
            "Rejection restores new outputs and reclaims variable state in bounded steps.",
            "Restoration loads retained progress; it never initializes that operation again.",
            "This private phase publishes no accepted prefix and releases no source custody.",
        ),
        limitations=(
            "Admission/indexing and existing local schema/byte/hash atoms are separate delegates.",
            "Unsupported row kinds and post-row checks remain separate phases.",
            "Allocation, B-tree byte comparisons, physical I/O, waits and fsync stay separate.",
            "Whole-session recovery needs an owner-retained crash-safe workspace and source.",
        ),
    )


def _result_declaration() -> Declaration:
    unit = _declaration()
    return replace(
        unit,
        id="riverhog-provenance.sqlite-semantic-result.growth/v1",
        subjects=("riverhog_provenance._generic_semantics.GenericSemantics.result",),
        scope="native-sqlite-fenced-semantic-terminal-result",
        starts_at="load the completed exact-source semantic checkpoint and retained failure frame",
        ends_at="return success or reconstruct one authentic native error without changing state",
        bounds=tuple(
            replace(
                bound,
                relation="equal",
                expression=Expression("constant", value=0, unit=COUNT),
                meaning="Terminal results neither change state nor traverse graph adjacency.",
            )
            if bound.metric in {"changed_rows", "adjacency_rows"}
            else bound
            for bound in unit.bounds
        ),
        conditions=unit.conditions
        + (
            "step_budget is one retained failure frame, and byte_budget equals terminal admission.",
            "Terminal admission is eight native entry envelopes plus 8192 control octets.",
        ),
        invariants=unit.invariants
        + (
            "Earlier outputs, duplicate keys and traversal frames are reclaimed before rejection.",
            "The current immutable atom supplies correlations independently of the desired error.",
        ),
    )


def install(db: sqlite3.Connection) -> None:
    db.execute(
        "CREATE TABLE journal_generic_semantics(singleton INTEGER PRIMARY KEY CHECK(singleton=1), "
        "algorithm TEXT NOT NULL, attempt_id TEXT NOT NULL, binding BLOB NOT NULL, "
        "prefix_attempt TEXT NOT NULL, prefix_binding BLOB NOT NULL, prefix TEXT NOT NULL, "
        "checkpoint TEXT NOT NULL)"
    ).close()
    db.execute(
        "CREATE TABLE generic_semantic_frames(attempt_id TEXT, frame_id INTEGER, parent INTEGER, "
        "value TEXT NOT NULL, PRIMARY KEY(attempt_id,frame_id))"
    ).close()
    db.execute(
        "CREATE TABLE generic_semantic_keys(id INTEGER PRIMARY KEY, attempt_id TEXT, "
        "key BLOB NOT NULL, UNIQUE(attempt_id,key))"
    ).close()
    db.execute(
        "CREATE INDEX generic_semantic_cleanup ON generic_semantic_keys(attempt_id,id)"
    ).close()
    db.execute(
        "CREATE TABLE generic_semantic_outputs(id INTEGER PRIMARY KEY, attempt_id TEXT, "
        "name TEXT NOT NULL,key BLOB NOT NULL,finding TEXT,UNIQUE(attempt_id,name,key))"
    ).close()
    db.execute(
        "CREATE INDEX generic_semantic_output_cleanup ON generic_semantic_outputs(attempt_id,id)"
    ).close()


class _Meter:
    def __init__(self, budget: int) -> None:
        self.budget, self.used = budget, 0

    def claim(self, octets: int) -> None:
        if self.used + octets > self.budget:
            raise GenericSemanticsCapacity("byte budget does not admit the next semantic atom")
        self.used += octets


class _Objects(Mapping[str, dict[str, Any]]):
    def __init__(self, owner: GenericSemantics, meter: _Meter) -> None:
        self.owner, self.meter = owner, meter

    def __contains__(self, key: object) -> bool:
        return (
            self.owner._one(
                "SELECT 1 FROM assertions INDEXED BY semantic_reference_type "
                "WHERE object_id=? AND active=1",
                (key,),
            )
            is not None
        )

    def __getitem__(self, key: str) -> dict[str, Any]:
        row = self.owner._one(
            "SELECT object_id,type FROM assertions INDEXED BY semantic_reference_type "
            "WHERE object_id=? AND active=1",
            (key,),
        )
        if row is None:
            raise KeyError(key)
        self.meter.claim(sum(len(value.encode()) for value in row))
        # References consume identity/type only. Lifecycle activity fields
        # come from the separate fixed metadata projection.
        return {"id": row[0], "type": row[1]}

    def __iter__(self) -> Iterator[str]:
        raise RuntimeError("semantic units do not enumerate the active view")

    def __len__(self) -> int:
        raise RuntimeError("semantic units do not count the active view")


class _Output(dict[Any, Any]):
    def __init__(self, name: str, meter: _Meter, writes: dict[tuple[str, bytes], bytes]) -> None:
        self.name, self.meter, self.writes = name, meter, writes

    def __setitem__(self, key: bytes, value: Any) -> None:
        encoded = canonical_document(value)
        if key != encoded:
            raise RuntimeError("common semantic output differs from its immutable identity")
        stored_key = encode_index_key(key)
        self.meter.claim(4 * len(stored_key) + len(encoded))
        self.writes[self.name, stored_key] = encoded


class _FindingOutput(list[str]):
    def __init__(self, meter: _Meter, writes: dict[tuple[str, bytes], bytes]) -> None:
        self.meter, self.writes = meter, writes

    def append(self, value: str) -> None:
        encoded = canonical_json_bytes(value)
        self.meter.claim(5 * len(encoded))
        self.writes["finding", encoded] = encoded


class _Lifecycle(MutableMapping[str, dict[str, Any]]):
    """A state's first compatible effective event, without documentary payloads."""

    def __init__(
        self,
        owner: GenericSemantics,
        name: str,
        meter: _Meter,
        writes: dict[tuple[str, bytes], bytes],
    ) -> None:
        self.owner, self.name, self.meter, self.writes = owner, name, meter, writes

    @staticmethod
    def _decode(encoded: bytes) -> dict[str, Any]:
        try:
            if type(encoded) is not bytes or len(encoded) > _LIFECYCLE_OCTETS:
                raise ValueError("metadata envelope")
            value = json.loads(encoded)
            if type(value) is not dict or set(value) not in (
                {"activity_id"},
                {"activity_id", "at"},
            ):
                raise ValueError("metadata shape")
            require_canonical_uuid_urn(value["activity_id"])
            if "at" in value and (type(value["at"]) is not str or len(value["at"]) > 30):
                raise ValueError("timestamp envelope")
            if canonical_document(value) != encoded:
                raise ValueError("metadata encoding")
            return value
        except (ValueError, TypeError, KeyError) as exc:
            raise RuntimeError("invalid retained lifecycle metadata") from exc

    def __getitem__(self, key: str) -> dict[str, Any]:
        encoded_key = b"j" + canonical_json_bytes(key)
        self.meter.claim(len(encoded_key))
        pending = self.writes.get((self.name, encoded_key))
        if pending is not None:
            return self._decode(pending)
        size = self.owner._one(
            "SELECT length(value) FROM maps WHERE name=? AND key=?",
            (self.name, encoded_key),
        )
        if size is None:
            raise KeyError(key)
        if type(size[0]) is not int or not 0 <= size[0] <= _LIFECYCLE_OCTETS:
            raise RuntimeError("invalid retained lifecycle metadata envelope")
        self.meter.claim(size[0])
        row = self.owner._one(
            "SELECT value FROM maps WHERE name=? AND key=?",
            (self.name, encoded_key),
        )
        if row is None or len(row[0]) != size[0]:
            raise RuntimeError("retained lifecycle metadata changed outside its fence")
        return self._decode(row[0])

    def __setitem__(self, key: str, value: dict[str, Any]) -> None:
        if key != value["state"]["object_id"]:
            raise RuntimeError("lifecycle event differs from its state key")
        encoded_key = b"j" + canonical_json_bytes(key)
        encoded = canonical_document(
            {field: value[field] for field in ("activity_id", "at") if field in value}
        )
        self._decode(encoded)
        self.meter.claim(4 * len(encoded_key) + len(encoded))
        self.writes[self.name, encoded_key] = encoded

    def __delitem__(self, key: str) -> None:
        raise RuntimeError("semantic undo owns lifecycle removal")

    def __iter__(self) -> Iterator[str]:
        raise RuntimeError("semantic units do not enumerate lifecycle events")

    def __len__(self) -> int:
        raise RuntimeError("semantic units do not count lifecycle events")


class _BindingEnds(MutableSet[str]):
    def __init__(
        self, owner: GenericSemantics, meter: _Meter, writes: dict[tuple[str, bytes], bytes]
    ) -> None:
        self.owner, self.meter, self.writes = owner, meter, writes

    def __contains__(self, value: object) -> bool:
        if not isinstance(value, str):
            return False
        key = b"j" + canonical_json_bytes(value)
        self.meter.claim(len(key))
        return ("ends", key) in self.writes or self.owner._one(
            "SELECT 1 FROM maps WHERE name='ends' AND key=?", (key,)
        ) is not None

    def add(self, value: str) -> None:
        key = b"j" + canonical_json_bytes(value)
        self.meter.claim(4 * len(key) + 4)
        self.writes["ends", key] = b"null"

    def discard(self, value: str) -> None:
        raise RuntimeError("semantic undo owns binding-end removal")

    def __iter__(self) -> Iterator[str]:
        raise RuntimeError("semantic units do not enumerate binding ends")

    def __len__(self) -> int:
        raise RuntimeError("semantic units do not count binding ends")


class _DeliverySlots(MutableSet[tuple[str, bytes]]):
    def __init__(
        self, owner: GenericSemantics, meter: _Meter, writes: dict[tuple[str, bytes], bytes]
    ) -> None:
        self.owner, self.meter, self.writes = owner, meter, writes

    def __contains__(self, value: object) -> bool:
        if not isinstance(value, tuple):
            return False
        key = encode_index_key(value)
        self.meter.claim(len(key))
        return ("slots", key) in self.writes or self.owner._one(
            "SELECT 1 FROM maps WHERE name='slots' AND key=?", (key,)
        ) is not None

    def add(self, value: tuple[str, bytes]) -> None:
        key = encode_index_key(value)
        self.meter.claim(4 * len(key) + 4)
        self.writes["slots", key] = b"null"

    def discard(self, value: tuple[str, bytes]) -> None:
        raise RuntimeError("semantic undo owns delivery-slot removal")

    def __iter__(self) -> Iterator[tuple[str, bytes]]:
        raise RuntimeError("semantic units do not enumerate delivery slots")

    def __len__(self) -> int:
        raise RuntimeError("semantic units do not count delivery slots")


class _CaptureTargets(MutableMapping[str, str]):
    def __init__(
        self, owner: GenericSemantics, meter: _Meter, writes: dict[tuple[str, bytes], bytes]
    ) -> None:
        self.owner, self.meter, self.writes = owner, meter, writes

    def __getitem__(self, key: str) -> str:
        encoded_key = b"j" + canonical_json_bytes(key)
        self.meter.claim(len(encoded_key))
        pending = self.writes.get(("captures", encoded_key))
        if pending is None:
            row = self.owner._one(
                "SELECT substr(CAST(value AS BLOB),1,48),typeof(value) FROM maps "
                "WHERE name='captures' AND key=?",
                (encoded_key,),
            )
            if row is None:
                raise KeyError(key)
            pending, storage = row
            if storage != "blob":
                raise RuntimeError("invalid retained capture target storage")
        self.meter.claim(len(pending))
        try:
            value = json.loads(pending)
            if type(value) is not str:
                raise ValueError("capture target must be a UUID string")
            require_canonical_uuid_urn(value)
            if canonical_json_bytes(value) != pending:
                raise ValueError("noncanonical capture target")
        except (ValueError, TypeError, UnicodeError) as exc:
            raise RuntimeError("invalid retained capture target metadata") from exc
        return str(value)

    def __setitem__(self, key: str, value: str) -> None:
        require_canonical_uuid_urn(key)
        require_canonical_uuid_urn(value)
        encoded_key, encoded = b"j" + canonical_json_bytes(key), canonical_json_bytes(value)
        self.meter.claim(4 * len(encoded_key) + len(encoded))
        self.writes["captures", encoded_key] = encoded

    def __delitem__(self, key: str) -> None:
        raise RuntimeError("semantic undo owns capture-target removal")

    def __iter__(self) -> Iterator[str]:
        raise RuntimeError("semantic units do not enumerate capture targets")

    def __len__(self) -> int:
        raise RuntimeError("semantic units do not count capture targets")


class _ProjectedValidator(_Validator):
    def __init__(
        self,
        owner: GenericSemantics,
        meter: _Meter,
        outputs: dict[tuple[str, bytes], bytes],
        edge_result: SemanticEdge | None = None,
    ) -> None:
        self.owner, self.meter = owner, meter
        self.edge_result = edge_result
        self.catalog, self.journal_id = owner.catalog, owner.source.journal_id
        self.require_profiles = owner.source.require_profiles
        self.objects = _Objects(owner, meter)
        self.externals = _Output("externals", meter, outputs)
        self.profiles = _Output("profiles", meter, outputs)
        self.findings = _FindingOutput(meter, outputs)
        self.capture_targets = _CaptureTargets(owner, meter, outputs)
        self.binding_ends = _BindingEnds(owner, meter, outputs)
        self.delivery_slots = _DeliverySlots(owner, meter, outputs)
        self.lifecycle = {
            name: _Lifecycle(owner, name, meter, outputs) for name in ("generation", "invalidation")
        }

    def _graph_edge(self, kind: str, source: str, target: str) -> None:
        if self.edge_result is None:
            raise _GraphRequest
        if (kind, source, target) != (
            self.edge_result.kind,
            self.edge_result.source,
            self.edge_result.target,
        ):
            raise RuntimeError("semantic graph result differs from its admitted row")
        self.edge_result.result()

    def _comparison_node(self, identity: str) -> dict[str, Any]:
        value = read_fixity_metadata(self.owner.db, identity)
        self.meter.claim(256)
        return value

    def _relation_node(self, identity: str, expected: str) -> dict[str, Any]:
        activity, state = read_relation_metadata(self.owner.db, identity, expected)
        self.meter.claim(len(activity.encode()) + len(state.encode()) + 128)
        return {"activity_id": activity, "state": {"object_id": state}}

    def _description_node(self, identity: str, *types: str) -> dict[str, Any]:
        value = read_description_metadata(self.owner.db, identity, *types)
        self.meter.claim(sum(len(item.encode()) for item in value.values()))
        return value

    def _observation_node(self, identity: str, expected: str) -> dict[str, Any]:
        value = read_observation_metadata(self.owner.db, identity, expected)
        self.meter.claim(384)
        return value

    def _observation_observer(self, identity: str, agent_id: str) -> bool:
        self.meter.claim(384)
        return has_observer(self.owner.db, identity, agent_id)

    def _observation_coverage_status(
        self, coverage: ObservationCoverage, item: Mapping[str, Any]
    ) -> str | None:
        self.meter.claim(len(item["profile_id"].encode()) + len(item["category"].encode()))
        return coverage.status(item)

    def _lifecycle_activity(self, identity: str) -> dict[str, Any]:
        value = read_activity(self.owner.db, identity)
        self.meter.claim(sum(len(item.encode()) for item in value.values()))
        return value

    def _binding_time(self, identity: str) -> str | None:
        value = read_binding_time(self.owner.db, identity)
        self.meter.claim(len(identity.encode()) + (len(value.encode()) if value is not None else 0))
        return value

    def _delivery_context(self, identity: str) -> dict[str, Any]:
        value = read_delivery_metadata(self.owner.db, identity, "context")
        self.meter.claim(len(identity.encode()) + len(value.encode()))
        return {"id": identity, "kind": value}

    def _locator_observation(self, identity: str) -> dict[str, Any]:
        value = read_locator_observation(self.owner.db, identity)
        self.meter.claim(256)
        return value

    def _delivery_observation(self, identity: str) -> dict[str, Any]:
        value = read_delivery_metadata(self.owner.db, identity, "observation")
        self.meter.claim(len(identity.encode()) + len(value.encode()))
        return {"state": {"object_id": value}}


class _Keys(MutableSet[bytes]):
    def __init__(self, owner: GenericSemantics, meter: _Meter) -> None:
        self.owner, self.meter = owner, meter
        self.pending: set[bytes] = set()

    def __contains__(self, value: object) -> bool:
        if not isinstance(value, bytes):
            return False
        self.meter.claim(len(value))
        return (
            value in self.pending
            or self.owner._one(
                "SELECT 1 FROM generic_semantic_keys WHERE attempt_id=? AND key=?",
                (self.owner.attempt_id, value),
            )
            is not None
        )

    def add(self, value: bytes) -> None:
        self.meter.claim(len(value))
        self.pending.add(value)

    def discard(self, value: bytes) -> None:
        raise RuntimeError("semantic key cleanup owns bounded removal")

    def __iter__(self) -> Iterator[bytes]:
        raise RuntimeError("semantic units do not enumerate metadata keys")

    def __len__(self) -> int:
        raise RuntimeError("semantic units do not count metadata keys")


class _Stack:
    def __init__(self, owner: GenericSemantics, state: GenericState, meter: _Meter) -> None:
        self.owner, self.top, self.serial, self.meter = owner, state.top, state.serial, meter
        self.cache: dict[int, tuple[int | None, Frame]] = {}
        self.changes: dict[int, tuple[int | None, Frame] | None] = {}

    def _row(self, identity: int) -> tuple[int | None, Frame]:
        if identity not in self.cache:
            row = self.owner._one(
                "SELECT parent,substr(value,1,?) FROM generic_semantic_frames "
                "WHERE attempt_id=? AND frame_id=?",
                (_FRAME_OCTETS + 1, self.owner.attempt_id, identity),
            )
            if row is None or row[0] is not None and not 0 <= row[0] < identity:
                raise RuntimeError("missing or invalid generic semantic stack frame")
            self.cache[identity] = row[0], _decode_frame(row[1], self.owner.atom.nodes)
        return self.cache[identity]

    def peek(self) -> Frame | None:
        return None if self.top is None else self._row(self.top)[1]

    def identity(self) -> int:
        if self.top is None:
            raise RuntimeError("empty semantic stack")
        return self.top

    def replace(self, frame: Frame) -> None:
        if self.top is None:
            raise RuntimeError("empty semantic stack")
        self.meter.claim(_FRAME_OCTETS)
        row = self._row(self.top)[0], frame
        self.cache[self.top] = self.changes[self.top] = row

    def push(self, operation: str, node: int) -> None:
        self.meter.claim(_FRAME_OCTETS)
        identity = self.serial
        self.serial += 1
        row = self.top, Frame(operation, node)
        self.cache[identity] = self.changes[identity] = row
        self.top = identity

    def pop(self) -> None:
        if self.top is None:
            raise RuntimeError("empty semantic stack")
        self.meter.claim(_FRAME_OCTETS)
        identity, self.top = self.top, self._row(self.top)[0]
        self.changes[identity] = None


@dataclass(frozen=True)
class GenericBatch:
    binding: bytes
    attempt_id: str
    prefix: tuple[str, bytes, str]
    before_json: str
    after: GenericState
    writes: tuple[tuple[str, tuple[Any, ...]], ...]
    edge: EdgeAdmission | EdgeBatch | None = None


class GenericSemantics:
    def __init__(
        self,
        db: sqlite3.Connection,
        *,
        attempt_id: str,
        source_binding: SemanticSource,
        atom: GenericAtom,
        catalog: ContractCatalog,
        operation: str = "generic",
    ) -> None:
        if type(attempt_id) is not str or not attempt_id:
            raise ValueError("an explicit generic semantic attempt is required")
        if type(source_binding) is not SemanticSource or type(atom) is not GenericAtom:
            raise ValueError("exact admitted semantic source and row are required")
        if catalog.checkpoint_sha256() != source_binding.catalog_sha256:
            raise StaleGenericSemantics("semantic catalog differs from its execution pin")
        self.db, self.attempt_id, self.source, self.atom, self.catalog = (
            db,
            attempt_id,
            source_binding,
            atom,
            catalog,
        )
        row = atom.nodes.values[0]
        if operation not in {"generic", "row"} or (
            operation == "row" and row["type"] not in ROW_TYPES
        ):
            raise ValueError("an exact supported semantic root operation is required")
        self.operation = operation
        self.assertion_id, self.object_id, self.kind = row["assertion_id"], row["id"], row["type"]
        self.binding = canonical_json_bytes(
            {
                "algorithm": ALGORITHM,
                "operation": operation,
                "source_sha256": source_binding.sha256,
                "catalog_sha256": source_binding.catalog_sha256,
                "require_profiles": source_binding.require_profiles,
                "assertion_id": self.assertion_id,
                "object_id": self.object_id,
                "kind": self.kind,
                "row_sha256": atom.sha256,
            }
        )

    def _one(self, sql: str, parameters: tuple[Any, ...] = ()) -> Any:
        with closing(self.db.execute(sql, parameters)) as rows:
            return rows.fetchone()

    def _detached(self) -> None:
        if self.db.in_transaction:
            raise RuntimeError("generic semantic units require a closed owner transaction")

    def _prefix(self) -> tuple[str, bytes, str]:
        row = self._one(
            "SELECT substr(attempt_id,1,?),substr(binding,1,?),substr(checkpoint,1,?) "
            "FROM journal_prefix WHERE singleton=1",
            (_HEADER_OCTETS + 1, _BINDING_OCTETS + 1, _HEADER_OCTETS + 1),
        )
        if (
            row is None
            or type(row[0]) is not str
            or not row[0]
            or len(row[0]) > _HEADER_OCTETS
            or type(row[1]) is not bytes
            or len(row[1]) > _BINDING_OCTETS
        ):
            raise RuntimeError("missing or invalid semantic accepted-prefix manifest")
        prefix = PrefixState.from_json(row[2])
        if str(prefix.frame_count) != self.source.sequence or prefix.journal_id not in (
            None,
            self.source.journal_id,
        ):
            raise StaleGenericSemantics("semantic frame differs from its accepted prefix")
        return row[0], row[1], row[2]

    def _subject(self) -> None:
        row = self._one(
            "SELECT a.object_id,a.type,a.active FROM changes c LEFT JOIN assertions a "
            "INDEXED BY semantic_subject ON a.assertion_id=c.assertion_id "
            "WHERE c.assertion_id=?",
            (self.assertion_id,),
        )
        if row != (self.object_id, self.kind, 1):
            raise StaleGenericSemantics("common semantic subject is missing or changed")
        if self.catalog.checkpoint_sha256() != self.source.catalog_sha256:
            raise StaleGenericSemantics("semantic catalog was replaced")

    def initialize(self) -> GenericState:
        self._detached()
        self.db.execute("BEGIN IMMEDIATE").close()
        try:
            prefix = self._prefix()
            self._subject()
            old = self._one(
                "SELECT algorithm,attempt_id,substr(checkpoint,1,?) "
                "FROM journal_generic_semantics WHERE singleton=1",
                (_HEADER_OCTETS + 1,),
            )
            version = 0
            if old is not None:
                if old[0] != ALGORITHM:
                    raise RuntimeError("unknown common semantic algorithm")
                prior = GenericState.from_json(old[2])
                if old[1] == self.attempt_id or prior.phase != "done":
                    raise StaleGenericSemantics("common semantic attempt is already bound")
                version = prior.version + 1
            if (
                self._one("SELECT 1 FROM generic_semantic_frames LIMIT 1") is not None
                or self._one("SELECT 1 FROM generic_semantic_keys LIMIT 1") is not None
                or self._one("SELECT 1 FROM generic_semantic_outputs LIMIT 1") is not None
            ):
                raise RuntimeError("common semantic state was not reclaimed")
            state = GenericState(version=version)
            self.db.execute(
                "INSERT INTO journal_generic_semantics VALUES (1,?,?,?,?,?,?,?) "
                "ON CONFLICT(singleton) DO UPDATE SET algorithm=excluded.algorithm, "
                "attempt_id=excluded.attempt_id,binding=excluded.binding, "
                "prefix_attempt=excluded.prefix_attempt,prefix_binding=excluded.prefix_binding, "
                "prefix=excluded.prefix,checkpoint=excluded.checkpoint",
                (ALGORITHM, self.attempt_id, self.binding, *prefix, state.to_json()),
            ).close()
            self.db.execute(
                "INSERT INTO generic_semantic_frames VALUES (?,?,?,?)",
                (self.attempt_id, 0, None, _encode_frame(Frame(self.operation, 0))),
            ).close()
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return state

    def _load(self) -> tuple[GenericState, tuple[str, bytes, str]]:
        self._detached()
        row = self._one(
            "SELECT algorithm,substr(prefix_attempt,1,?),substr(prefix_binding,1,?), "
            "substr(prefix,1,?),substr(checkpoint,1,?) FROM journal_generic_semantics "
            "WHERE singleton=1 AND attempt_id=? AND binding=?",
            (
                _HEADER_OCTETS + 1,
                _BINDING_OCTETS + 1,
                _HEADER_OCTETS + 1,
                _HEADER_OCTETS + 1,
                self.attempt_id,
                self.binding,
            ),
        )
        if row is None:
            raise StaleGenericSemantics("common semantic inputs were superseded")
        if row[0] != ALGORITHM:
            raise RuntimeError("unknown common semantic algorithm")
        prefix = self._prefix()
        if row[1:4] != prefix:
            raise StaleGenericSemantics("accepted prefix changed during common semantics")
        self._subject()
        return GenericState.from_json(row[4]), prefix

    def load(self) -> GenericState:
        return self._load()[0]

    def _edge_work(self, state: GenericState, prefix: tuple[str, bytes, str]) -> SemanticEdge:
        if (
            self.operation != "row"
            or self.kind not in {"derivation", "specialization"}
            or state.edge_run is None
        ):
            raise RuntimeError("common semantic state has no matching graph child")
        value = self.atom.nodes.values[0]
        left, right = (
            ("used_state", "generated_state")
            if self.kind == "derivation"
            else ("specific", "general")
        )
        attempt = hashlib.sha256(
            canonical_json_bytes(
                {
                    "parent_attempt": self.attempt_id,
                    "parent_binding_sha256": hashlib.sha256(self.binding).hexdigest(),
                    "prefix_attempt": prefix[0],
                    "prefix_binding_sha256": hashlib.sha256(prefix[1]).hexdigest(),
                    "prefix": prefix[2],
                }
            )
        ).hexdigest()
        return SemanticEdge(
            self.db,
            kind=self.kind,
            run_id=state.edge_run,
            attempt_id=attempt,
            source_binding=self.source,
            assertion_id=self.assertion_id,
            object_id=self.object_id,
            source=value[left]["object_id"],
            target=value[right]["object_id"],
        )

    @staticmethod
    def _charge_edge(meter: _Meter, batch: EdgeAdmission | EdgeBatch, row_budget: int) -> None:
        # Graph rows contain native UUID keys; actual scalar/control and write
        # encodings retain their own widths rather than becoming archive caps.
        values: list[Any] = [
            batch.binding,
            batch.attempt_id,
            *batch.prefix,
            batch.after.to_json(),
            batch.graph_before.work.encoded(),
        ]
        if isinstance(batch, EdgeAdmission):
            if batch.prior is not None:
                values.extend(batch.prior)
        else:
            values.append(batch.before_json)
            if batch.graph_plan is not None:
                values.append(batch.graph_plan.work.encoded())
                for _statement, parameters in batch.graph_plan.writes:
                    values.extend(parameters)
        meter.claim(
            4 * 45 * row_budget
            + sum(
                len(value)
                if isinstance(value, bytes)
                else len(str(value).encode())
                if value is not None
                else 0
                for value in values
            )
        )

    def _program(
        self,
        stack: Stack,
        meter: _Meter,
        keys: _Keys,
        outputs: dict[tuple[str, bytes], bytes],
        *,
        edge_result: SemanticEdge | None = None,
    ) -> Program:
        return Program(
            _ProjectedValidator(self, meter, outputs, edge_result), self.atom.nodes, stack, keys
        )

    def prepare(
        self, *, step_budget: int = MAX_STEPS, byte_budget: int = _DEFAULT_BYTES
    ) -> GenericBatch:
        if type(step_budget) is not int or not 1 <= step_budget <= MAX_STEPS:
            raise ValueError("semantic step budget must be one to 64")
        if type(byte_budget) is not int or byte_budget < 16384:
            raise ValueError("semantic byte budget must admit fixed control metadata")
        state, prefix = self._load()
        after = state
        writes: list[tuple[str, tuple[Any, ...]]] = []
        edge: EdgeAdmission | EdgeBatch | None = None
        meter = _Meter(byte_budget)
        meter.claim(
            8192
            + 2
            * (
                len(self.binding)
                + sum(
                    len(value.encode()) if isinstance(value, str) else len(value)
                    for value in prefix
                )
            )
        )
        if state.phase == "walk":
            stack = _Stack(self, state, meter)
            outputs: dict[tuple[str, bytes], bytes] = {}
            keys = _Keys(self, meter)
            program = self._program(stack, meter, keys, outputs)
            phase, error_hash, failure = "walk", None, None
            edge_run = None
            for completed in range(step_budget):
                if stack.peek() is None:
                    break
                snapshot = (
                    stack.top,
                    stack.serial,
                    dict(stack.cache),
                    dict(stack.changes),
                    set(keys.pending),
                    dict(outputs),
                )
                try:
                    program.step()
                except (GenericSemanticsCapacity, ValueError, _GraphRequest) as error:
                    stack.top, stack.serial, stack.cache, stack.changes = snapshot[:4]
                    keys.pending.clear()
                    keys.pending.update(snapshot[4])
                    outputs.clear()
                    outputs.update(snapshot[5])
                    if isinstance(error, _GraphRequest):
                        run = self._one(
                            "SELECT substr(CAST(run_id AS BLOB),1,?),typeof(run_id) "
                            "FROM dag_runs WHERE kind=?",
                            (_HEADER_OCTETS + 1, self.kind),
                        )
                        if (
                            run is None
                            or type(run[0]) is not bytes
                            or run[1] != "text"
                            or not 1 <= len(run[0]) <= _HEADER_OCTETS
                        ):
                            raise RuntimeError(
                                "missing or invalid common semantic graph run"
                            ) from None
                        graph_run = run[0].decode("utf-8")
                        proposed = replace(state, phase="edge", edge_run=graph_run)
                        admission = self._edge_work(proposed, prefix).prepare_admission()
                        try:
                            self._charge_edge(meter, admission, 0)
                        except GenericSemanticsCapacity:
                            if completed == 0:
                                raise
                            break
                        phase, edge_run, edge = "edge", graph_run, admission
                        break
                    if isinstance(error, GenericSemanticsCapacity):
                        if completed == 0:
                            raise
                        break
                    failed_frame = stack.peek()
                    if failed_frame is None:
                        raise RuntimeError("semantic rejection lost its current atom") from error
                    phase, error_hash = "reject-frames", _error_identity(error)
                    failure = _encode_frame(failed_frame)
                    break
            if phase == "walk" and stack.top is None:
                phase = "keys"
            after = replace(
                state,
                phase=phase,
                top=stack.top if phase in {"walk", "edge"} else None,
                serial=stack.serial,
                error_sha256=error_hash,
                failure=failure,
                edge_run=edge_run,
                version=state.version + 1,
            )
            for identity, item in stack.changes.items():
                if item is None:
                    writes.append(
                        (
                            "DELETE FROM generic_semantic_frames WHERE attempt_id=? AND frame_id=?",
                            (self.attempt_id, identity),
                        )
                    )
                else:
                    parent, frame = item
                    writes.append(
                        (
                            "INSERT INTO generic_semantic_frames VALUES (?,?,?,?) "
                            "ON CONFLICT(attempt_id,frame_id) DO UPDATE SET "
                            "parent=excluded.parent,value=excluded.value",
                            (self.attempt_id, identity, parent, _encode_frame(frame)),
                        )
                    )
            for key in keys.pending:
                writes.append(
                    (
                        "INSERT INTO generic_semantic_keys(attempt_id,key) VALUES (?,?)",
                        (self.attempt_id, key),
                    )
                )
            for (name, key), value in outputs.items():
                writes.append(("@output", (name, key, value)))
        elif state.phase == "edge":
            stack = _Stack(self, state, meter)
            edge_frame = stack.peek()
            if edge_frame is None or edge_frame != Frame("row", 0, stage=2):
                raise RuntimeError("graph child differs from its parent continuation")
            child = self._edge_work(state, prefix)
            edge = child.prepare(row_budget=step_budget)
            self._charge_edge(meter, edge, step_budget)
            after = replace(state, version=state.version + 1)
            if edge.after.done and edge.after.to_json() == edge.before_json:
                try:
                    child.result()
                except ValueError as error:
                    after = replace(
                        after,
                        phase="reject-frames",
                        top=None,
                        error_sha256=_error_identity(error),
                        failure=_encode_frame(edge_frame),
                    )
                else:
                    writes.append(
                        (
                            "UPDATE generic_semantic_frames SET value=? "
                            "WHERE attempt_id=? AND frame_id=?",
                            (
                                _encode_frame(replace(edge_frame, stage=3)),
                                self.attempt_id,
                                state.top,
                            ),
                        )
                    )
                    after = replace(after, phase="walk", edge_run=None)
        elif state.phase == "reject-frames":
            with closing(
                self.db.execute(
                    "SELECT frame_id FROM generic_semantic_frames WHERE attempt_id=? "
                    "ORDER BY frame_id LIMIT ?",
                    (self.attempt_id, step_budget + 1),
                )
            ) as rows:
                values = rows.fetchall()
            for (identity,) in values[:step_budget]:
                try:
                    meter.claim(_FRAME_OCTETS)
                except GenericSemanticsCapacity:
                    if not writes:
                        raise
                    break
                writes.append(
                    (
                        "DELETE FROM generic_semantic_frames WHERE attempt_id=? AND frame_id=?",
                        (self.attempt_id, identity),
                    )
                )
            after = replace(
                state,
                phase="reject-keys" if len(writes) == len(values) else "reject-frames",
                version=state.version + 1,
            )
        elif state.phase in {"keys", "reject-keys"}:
            with closing(
                self.db.execute(
                    "SELECT id,length(key) FROM generic_semantic_keys "
                    "INDEXED BY generic_semantic_cleanup WHERE attempt_id=? ORDER BY id LIMIT ?",
                    (self.attempt_id, step_budget + 1),
                )
            ) as rows:
                values = rows.fetchall()
            for identity, length in values[:step_budget]:
                try:
                    meter.claim(length + 512)
                except GenericSemanticsCapacity:
                    if not writes:
                        raise
                    break
                writes.append(
                    (
                        "DELETE FROM generic_semantic_keys WHERE attempt_id=? AND id=?",
                        (self.attempt_id, identity),
                    )
                )
            phase = (
                ("outputs" if state.phase == "keys" else "reject-outputs")
                if (len(writes) == len(values))
                else state.phase
            )
            after = replace(state, phase=phase, version=state.version + 1)
        elif state.phase in {"outputs", "reject-outputs"}:
            with closing(
                self.db.execute(
                    "SELECT id,length(key) FROM generic_semantic_outputs "
                    "INDEXED BY generic_semantic_output_cleanup WHERE attempt_id=? "
                    "ORDER BY id LIMIT ?",
                    (self.attempt_id, step_budget + 1),
                )
            ) as rows:
                values = rows.fetchall()
            for identity, length in values[:step_budget]:
                try:
                    meter.claim((5 if state.phase == "reject-outputs" else 2) * length + 512)
                except GenericSemanticsCapacity:
                    if not writes:
                        raise
                    break
                writes.append(
                    (
                        "@rollback-output" if state.phase == "reject-outputs" else "@retire-output",
                        (identity,),
                    )
                )
            phase = (
                ("done" if state.phase == "outputs" else "rejected")
                if (len(writes) == len(values))
                else state.phase
            )
            after = replace(state, phase=phase, version=state.version + 1)
        return GenericBatch(
            self.binding, self.attempt_id, prefix, state.to_json(), after, tuple(writes), edge
        )

    def commit(self, batch: GenericBatch) -> GenericState:
        self._detached()
        if (batch.binding, batch.attempt_id) != (self.binding, self.attempt_id):
            raise StaleGenericSemantics("common semantic proposal belongs to another input")
        self.db.execute("BEGIN IMMEDIATE").close()
        try:
            if self._prefix() != batch.prefix:
                raise StaleGenericSemantics("accepted prefix changed before semantic commit")
            self._subject()
            if batch.after == GenericState.from_json(batch.before_json):
                if batch.writes or batch.edge is not None:
                    raise RuntimeError("completed semantic proposal contains writes")
                row = self._one(
                    "SELECT 1 FROM journal_generic_semantics WHERE singleton=1 "
                    "AND algorithm=? AND attempt_id=? AND binding=? AND prefix_attempt=? "
                    "AND prefix_binding=? AND prefix=? AND checkpoint=?",
                    (ALGORITHM, self.attempt_id, self.binding, *batch.prefix, batch.before_json),
                )
                if row is None:
                    raise StaleGenericSemantics("finished common semantic worker was replaced")
                self.db.commit()
                return batch.after
            with closing(
                self.db.execute(
                    "UPDATE journal_generic_semantics SET checkpoint=? WHERE singleton=1 "
                    "AND algorithm=? AND attempt_id=? AND binding=? AND prefix_attempt=? "
                    "AND prefix_binding=? AND prefix=? AND checkpoint=?",
                    (
                        batch.after.to_json(),
                        ALGORITHM,
                        self.attempt_id,
                        self.binding,
                        *batch.prefix,
                        batch.before_json,
                    ),
                )
            ) as cursor:
                if cursor.rowcount != 1:
                    raise StaleGenericSemantics("common semantic checkpoint changed")
            if isinstance(batch.edge, EdgeAdmission):
                self._edge_work(batch.after, batch.prefix)._admit(batch.edge)
            elif isinstance(batch.edge, EdgeBatch):
                before = GenericState.from_json(batch.before_json)
                self._edge_work(before, batch.prefix)._apply(batch.edge)
            for sql, parameters in batch.writes:
                if sql == "@output":
                    name, key, value = parameters
                    if name == "finding":
                        finding = json.loads(value)
                        if (
                            type(finding) is not str
                            or key != value
                            or canonical_json_bytes(finding) != value
                        ):
                            raise RuntimeError("semantic finding differs from its identity")
                        with closing(
                            self.db.execute(
                                "INSERT INTO findings VALUES (?,?) ON CONFLICT(key) DO NOTHING",
                                (finding, finding),
                            )
                        ) as cursor:
                            inserted = cursor.rowcount
                        if inserted:
                            self.db.execute(
                                "INSERT INTO generic_semantic_outputs(attempt_id,name,key,finding) "
                                "VALUES (?,?,?,?)",
                                (self.attempt_id, name, key, finding),
                            ).close()
                        elif (
                            self._one(
                                "SELECT 1 FROM findings WHERE key=? AND value=?", (finding, finding)
                            )
                            is None
                        ):
                            raise RuntimeError(
                                "retained semantic finding differs from its identity"
                            )
                        continue
                    with closing(
                        self.db.execute(
                            "INSERT INTO maps VALUES (?,?,?) ON CONFLICT(name,key) DO NOTHING",
                            parameters,
                        )
                    ) as cursor:
                        inserted = cursor.rowcount
                    if inserted:
                        self.db.execute(
                            "INSERT INTO generic_semantic_outputs(attempt_id,name,key) "
                            "VALUES (?,?,?)",
                            (self.attempt_id, name, key),
                        ).close()
                    elif (
                        self._one(
                            "SELECT 1 FROM maps WHERE name=? AND key=? AND value=?",
                            (name, key, value),
                        )
                        is None
                    ):
                        raise RuntimeError("retained semantic output differs from its identity")
                elif sql in {"@rollback-output", "@retire-output"}:
                    if sql == "@rollback-output":
                        row = self._one(
                            "SELECT name FROM generic_semantic_outputs WHERE id=? AND attempt_id=?",
                            (*parameters, self.attempt_id),
                        )
                        if row is None:
                            raise RuntimeError("semantic undo no longer owns its output")
                        delete = (
                            "DELETE FROM findings WHERE key=value AND key=(SELECT finding "
                            "FROM generic_semantic_outputs WHERE id=? AND attempt_id=? "
                            "AND name='finding')"
                            if row[0] == "finding"
                            else "DELETE FROM maps WHERE (name,key)=(SELECT name,key "
                            "FROM generic_semantic_outputs WHERE id=? AND attempt_id=?)"
                        )
                        with closing(
                            self.db.execute(
                                delete,
                                (*parameters, self.attempt_id),
                            )
                        ) as cursor:
                            if cursor.rowcount != 1:
                                raise RuntimeError("semantic undo no longer owns its output")
                    with closing(
                        self.db.execute(
                            "DELETE FROM generic_semantic_outputs WHERE id=? AND attempt_id=?",
                            (*parameters, self.attempt_id),
                        )
                    ) as cursor:
                        if cursor.rowcount != 1:
                            raise RuntimeError("semantic output reclamation lost its row")
                else:
                    self.db.execute(sql, parameters).close()
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return batch.after

    @declare_growth(_declaration())
    def advance(
        self, *, step_budget: int = MAX_STEPS, byte_budget: int = _DEFAULT_BYTES
    ) -> GenericState:
        return self.commit(self.prepare(step_budget=step_budget, byte_budget=byte_budget))

    @declare_growth(_result_declaration())
    def result(self) -> None:
        state, _prefix = self._load()
        if not state.done:
            raise RuntimeError("common semantics remain quarantined")
        if (
            self._one(
                "SELECT 1 FROM generic_semantic_frames WHERE attempt_id=? LIMIT 1",
                (self.attempt_id,),
            )
            is not None
            or self._one(
                "SELECT 1 FROM generic_semantic_keys WHERE attempt_id=? LIMIT 1", (self.attempt_id,)
            )
            is not None
            or self._one(
                "SELECT 1 FROM generic_semantic_outputs WHERE attempt_id=? LIMIT 1",
                (self.attempt_id,),
            )
            is not None
        ):
            raise RuntimeError("common semantic completion retains pending obligations")
        if state.phase == "rejected":
            meter = _Meter(_RESULT_BYTES)
            if state.failure is None:
                raise RuntimeError("semantic rejection lost its failing frame")
            frame = _decode_frame(state.failure, self.atom.nodes)
            stack = MemoryStack(frame.operation)
            stack.frames = [frame]
            keys = _Keys(self, meter)
            if frame.operation == "generic" and frame.stage == 3:
                value = self.atom.nodes.values[frame.node]["metadata"][frame.cursor]
                keys.pending.add(Program.metadata_key(value))
            elif frame.operation == "content" and frame.stage == 0:
                values = self.atom.nodes.values[frame.node]["digests"]
                if frame.cursor >= len(values):
                    raise RuntimeError("corrupt retained content rejection")
                value = values[frame.cursor]
                keys.pending.add(Program.content_key(stack.identity(), value))
            elif (
                frame.operation == "row"
                and self.kind in {"reported_description", "observation"}
                and frame.stage == 4
            ):
                coverage = self.atom.nodes.values[frame.node].get("coverage", [])
                if frame.cursor >= len(coverage):
                    raise RuntimeError("corrupt retained coverage rejection")
                keys.pending.add(Program.coverage_key(stack.identity(), coverage[frame.cursor]))
            edge_result = self._edge_work(state, _prefix) if state.edge_run is not None else None
            if (
                frame.operation == "row"
                and self.kind in {"derivation", "specialization"}
                and frame.stage == 2
                and edge_result is None
            ):
                raise RuntimeError("retained graph rejection lost its child")
            program = self._program(stack, meter, keys, {}, edge_result=edge_result)
            try:
                program.step()
            except ValueError as error:
                if _error_identity(error) != state.error_sha256:
                    raise RuntimeError(
                        "retained semantic rejection differs from its atom"
                    ) from error
                raise
            raise RuntimeError("retained semantic rejection no longer fails")
