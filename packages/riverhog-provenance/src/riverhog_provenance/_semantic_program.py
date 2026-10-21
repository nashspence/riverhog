"""One explicit stack for native common assertion semantics.

Each transition visits one control frame, one reference, or one admitted local
atom. The stack and metadata-key set have interchangeable memory and SQLite
owners; neither traversal depends on replaying earlier transitions.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, MutableSet
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, Protocol, cast

from riverhog_growth_contracts import (
    COUNT,
    SCALAR,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)
from riverhog_provenance_contracts import PROFILE, canonical_document

from ._observation_coverage import ObservationCoverage
from .errors import ProvenanceValidationError, UnresolvedContractError

if TYPE_CHECKING:
    from .graph import _Validator

ROW_STAGES = {
    "occurrence": 2,
    "state": 3,
    "activity": 8,
    "continuity": 3,
    "custody_assertion": 3,
    "availability_assertion": 3,
    "journal_subject": 2,
    "extension": 2,
    "usage": 4,
    "generation": 4,
    "invalidation": 4,
    "locator_binding_end": 3,
    "delivery_association": 5,
    "reported_description": 6,
    "observation": 17,
    "derivation": 8,
    "specialization": 3,
}
ROW_TYPES = frozenset(ROW_STAGES)


@dataclass(frozen=True)
class Frame:
    operation: str
    node: int
    stage: int = 0
    cursor: int = 0


def _index_declaration() -> Declaration:
    items = Expression("dimension", name="input_items")
    return Declaration(
        id="riverhog-provenance.semantic-atom-index.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance._semantic_program.Nodes.__init__",),
        scope="predecoded-json-semantic-container-index",
        starts_at="receive one owner-admitted immutable JSON tree",
        ends_at="retain one direct index per distinct container without copying scalar payloads",
        dimensions=(Dimension("input_items", COUNT, "Root plus actual JSON value slots."),),
        bounds=(
            Bound(
                "indexed_containers",
                COUNT,
                "upper",
                items,
                "At most one retained index row per input item, including empty containers.",
            ),
            Bound(
                "python_opcode_events",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        Expression("constant", value=8192, unit=COUNT),
                        Expression(
                            "product",
                            terms=(
                                Expression("constant", value=512, unit=SCALAR),
                                items,
                            ),
                        ),
                    ),
                ),
                "Bounded native Python control and one visit per input container/value slot.",
            ),
        ),
        conditions=(
            "The owner supplies immutable native JSON dict/list containers, without cycles.",
            "The measured item count includes root and every value slot, including scalars.",
        ),
        invariants=(
            "Stable decoded traversal order determines direct source-node identities.",
            "Advancing the semantic interpreter does not reconstruct this index.",
        ),
        limitations=(
            "JSON decoding, canonicalization, schema and hashing are separate admitted-atom costs.",
            "The index retains existing container references; it is not a memory or archive quota.",
            "Ordinary restart may reconstruct at most its current owner-admitted atom.",
            "Opcode observations cover this Python index kernel, not native allocation or I/O.",
        ),
    )


class Nodes:
    """Index an already admitted immutable JSON atom once, outside transactions."""

    @declare_growth(_index_declaration())
    def __init__(self, value: Mapping[str, Any]) -> None:
        self.values: list[Any] = []
        self.identities: dict[int, int] = {}
        pending: list[Any] = [value]
        while pending:
            item = pending.pop()
            if id(item) in self.identities:
                continue
            self.identities[id(item)] = len(self.values)
            self.values.append(item)
            if isinstance(item, Mapping):
                children = list(item.values())
            elif isinstance(item, list):
                children = item
            else:
                continue
            pending.extend(
                child for child in reversed(children) if isinstance(child, (Mapping, list))
            )

        self.observation = (
            ObservationCoverage(value) if value.get("type") == "observation" else None
        )

    def identity(self, value: Any) -> int:
        return self.identities[id(value)]


class Stack(Protocol):
    def peek(self) -> Frame | None: ...

    def identity(self) -> int: ...

    def replace(self, frame: Frame) -> None: ...

    def push(self, operation: str, node: int) -> None: ...

    def pop(self) -> None: ...


class MemoryStack:
    def __init__(self, operation: str) -> None:
        self.frames = [Frame(operation, 0)]
        self.identities = [0]
        self.serial = 1

    def peek(self) -> Frame | None:
        return self.frames[-1] if self.frames else None

    def identity(self) -> int:
        return self.identities[-1]

    def replace(self, frame: Frame) -> None:
        self.frames[-1] = frame

    def push(self, operation: str, node: int) -> None:
        self.frames.append(Frame(operation, node))
        self.identities.append(self.serial)
        self.serial += 1

    def pop(self) -> None:
        self.frames.pop()
        self.identities.pop()


class Program:
    def __init__(
        self,
        validator: _Validator,
        nodes: Nodes,
        stack: Stack,
        native_keys: MutableSet[bytes],
    ) -> None:
        self.validator, self.nodes, self.stack = validator, nodes, stack
        self.native_keys = native_keys

    @staticmethod
    def content_key(scope: int, item: Mapping[str, Any]) -> bytes:
        return canonical_document(
            {
                "content": str(scope),
                "algorithm": item["algorithm"],
                "algorithm_uri": item.get("algorithm_uri", ""),
            }
        )

    @staticmethod
    def coverage_key(scope: int, item: Mapping[str, Any]) -> bytes:
        return canonical_document(
            {"coverage": str(scope), "profile_id": item["profile_id"], "category": item["category"]}
        )

    @staticmethod
    def metadata_key(item: Mapping[str, Any]) -> bytes:
        from .graph import _name_identity, _source_identity

        return canonical_document(
            {
                "profile_id": item["profile_id"],
                "category": item["category"],
                "namespace": item.get("namespace", ""),
                "name": _name_identity(item["name"]),
                "source": _source_identity(item["source"]),
            }
        )

    def _next(self, frame: Frame) -> None:
        self.stack.replace(replace(frame, stage=frame.stage + 1, cursor=0))

    def _child(self, frame: Frame, operation: str, value: Any) -> None:
        self._next(frame)
        self.stack.push(operation, self.nodes.identity(value))

    def _items(self, frame: Frame, values: list[Any], operation: str) -> None:
        if frame.cursor < len(values):
            self.stack.replace(replace(frame, cursor=frame.cursor + 1))
            self.stack.push(operation, self.nodes.identity(values[frame.cursor]))
        else:
            self._next(frame)

    def _observation(self, frame: Frame, value: Mapping[str, Any]) -> None:
        validator, stage = self.validator, frame.stage
        coverage = self.nodes.observation
        if coverage is None:
            raise RuntimeError("observation frame has no admitted coverage index")
        if stage == 6:
            if (
                value["state"]["scope"] != "local"
                or validator._observation_node(value["state"]["object_id"], "state")["extent"][
                    "kind"
                ]
                == "unknown"
            ):
                raise ProvenanceValidationError("direct observation requires a local bounded state")
        elif stage == 7:
            if (
                validator._observation_node(value["capture_id"], "activity")["kind"]
                != "observation"
            ):
                raise ProvenanceValidationError(
                    "observation capture is not an observation activity"
                )
        elif stage == 8:
            if validator._observation_node(value["capture_id"], "activity")["outcome"] not in (
                "success",
                "partial",
            ):
                raise ProvenanceValidationError(
                    "a failed capture cannot establish complete primary fixity"
                )
        elif stage == 9:
            state_id = value["state"]["object_id"]
            if validator.capture_targets.setdefault(value["capture_id"], state_id) != state_id:
                raise ProvenanceValidationError("one capture cannot observe distinct states")
        elif stage == 10:
            if not coverage.primary_complete:
                raise ProvenanceValidationError(
                    "direct observation requires complete primary-content coverage"
                )
        elif stage == 11:
            if (
                coverage.incomplete
                and validator._observation_node(value["capture_id"], "activity")["outcome"]
                != "partial"
            ):
                raise ProvenanceValidationError(
                    "incomplete coverage requires partial capture outcome"
                )
        elif stage == 12:
            evidence = value["evidence"]
            if frame.cursor == len(evidence):
                raise ProvenanceValidationError(
                    "observation lacks attributed direct measurement evidence"
                )
            item = evidence[frame.cursor]
            if item["basis"] == "direct_measurement" and validator._observation_observer(
                value["capture_id"], item["asserted_by_agent_id"]
            ):
                self._next(frame)
            else:
                self.stack.replace(replace(frame, cursor=frame.cursor + 1))
            return
        elif stage == 13:
            consistency = value["consistency"]
            if "snapshot_identifier" in consistency:
                self._child(frame, "identifier", consistency["snapshot_identifier"])
                return
        elif stage == 14:
            if (
                value["consistency"]["level"] == "immutable_snapshot"
                and not value["capabilities"]["stable_version_selection"]
            ):
                raise ProvenanceValidationError("snapshot claim requires stable-version capability")
        elif stage == 15:
            if value.get("metadata") and not value["capabilities"]["native_metadata"]:
                raise ProvenanceValidationError(
                    "native evidence contradicts source capability declaration"
                )
        elif stage == 16:
            metadata = value.get("metadata", [])
            if frame.cursor < len(metadata):
                item = metadata[frame.cursor]
                status = validator._observation_coverage_status(coverage, item)
                if status is None:
                    raise ProvenanceValidationError(
                        "native metadata has no declared coverage category"
                    )
                if item["status"] == "unreadable" and status not in ("partial", "failed"):
                    raise ProvenanceValidationError(
                        "unreadable metadata requires partial/failed coverage"
                    )
                if status in ("not_exposed", "not_applicable", "not_requested"):
                    raise ProvenanceValidationError("metadata contradicts its declared coverage")
                self.stack.replace(replace(frame, cursor=frame.cursor + 1))
                return
        else:
            self.stack.pop()
            return
        self._next(frame)

    def _row(self, frame: Frame, value: Mapping[str, Any]) -> None:
        from .graph import ENTITY_TYPES, _name_identity, time_ns

        validator, stage, kind = self.validator, frame.stage, value["type"]
        if kind == "occurrence":
            if stage == 0:
                validator.node(value["artifact_id"], "artifact")
            elif stage == 1:
                if "source_context_id" in value:
                    validator.node(value["source_context_id"], "context")
            else:
                self.stack.pop()
                return
        elif kind == "state":
            if stage == 0:
                validator.node(value["occurrence_id"], "occurrence")
            elif stage in (1, 2):
                extent = value["extent"]
                if extent["kind"] == "segment":
                    key = "start_boundary" if stage == 1 else "end_boundary"
                    self._child(frame, "typed", extent[key])
                    return
            else:
                self.stack.pop()
                return
        elif kind == "activity":
            if stage < 2:
                key = "started_at" if stage == 0 else "ended_at"
                if key in value:
                    time_ns(value[key])
            elif stage == 2:
                if "started_at" in value and "ended_at" in value:
                    if time_ns(value["started_at"]) > time_ns(value["ended_at"]):
                        raise ProvenanceValidationError("activity ends before it starts")
            elif stage in (3, 4, 5):
                key, operation = (
                    ("associations", "association"),
                    ("contexts", "activity_context"),
                    ("details", "profile"),
                )[stage - 3]
                self._items(
                    frame, value[key] if key == "associations" else value.get(key, []), operation
                )
                return
            elif stage == 6:
                if "configuration" in value:
                    self._child(frame, "profile", value["configuration"])
                    return
            elif stage == 7:
                if "configuration" in value:
                    actual = hashlib.sha256(
                        canonical_document(value["configuration"]["data"])
                    ).hexdigest()
                    if value["configuration_sha256"] != actual:
                        raise ProvenanceValidationError("capture configuration digest mismatch")
            else:
                self.stack.pop()
                return
        elif kind in ("usage", "generation", "invalidation"):
            if stage == 0:
                validator.reference(value["state"], "state")
            elif stage == 1:
                activity = validator._lifecycle_activity(value["activity_id"])
                if kind == "generation" and activity["kind"] == "observation":
                    raise ProvenanceValidationError("capture does not generate its observed state")
            elif stage == 2:
                if kind != "usage":
                    validator._unique_lifecycle(
                        validator.lifecycle[kind], cast(dict[str, Any], value), kind
                    )
            elif stage == 3:
                if "at" in value:
                    validator._within_activity(
                        value["at"], validator._lifecycle_activity(value["activity_id"])
                    )
            else:
                self.stack.pop()
                return
        elif kind == "derivation":
            if stage in (0, 1):
                key = "used_state" if stage == 0 else "generated_state"
                validator.reference(value[key], "state")
            elif stage == 2:
                validator._graph_edge(
                    kind, value["used_state"]["object_id"], value["generated_state"]["object_id"]
                )
            elif "activity_id" not in value:
                self.stack.pop()
                return
            elif stage == 3:
                validator.node(value["activity_id"], "activity")
            elif stage in (4, 5):
                relation = "usage" if stage == 4 else "generation"
                validator.node(value[relation + "_id"], relation)
            elif stage in (6, 7):
                relation, endpoint = (
                    ("usage", "used_state") if stage == 6 else ("generation", "generated_state")
                )
                record = validator._relation_node(value[relation + "_id"], relation)
                if (
                    record["activity_id"] != value["activity_id"]
                    or record["state"]["object_id"] != value[endpoint]["object_id"]
                ):
                    raise ProvenanceValidationError(
                        "qualified derivation does not match usage/generation"
                    )
            else:
                self.stack.pop()
                return
        elif kind == "specialization":
            if stage in (0, 1):
                validator.reference(value["specific" if stage == 0 else "general"], *ENTITY_TYPES)
            elif stage == 2:
                validator._graph_edge(
                    kind, value["specific"]["object_id"], value["general"]["object_id"]
                )
            else:
                self.stack.pop()
                return
        elif kind == "locator_binding_end":
            if stage == 0:
                validator.node(value["binding_id"], "locator_binding")
            elif stage == 1:
                if value["binding_id"] in validator.binding_ends:
                    raise ProvenanceValidationError(
                        "duplicate effective end for one locator binding"
                    )
                validator.binding_ends.add(value["binding_id"])
            elif stage == 2:
                bound = validator._binding_time(value["binding_id"])
                if bound and time_ns(value["at"]) < time_ns(bound):
                    raise ProvenanceValidationError("binding end precedes the known binding")
            else:
                self.stack.pop()
                return
        elif kind == "delivery_association":
            if stage == 0:
                context = validator._delivery_context(value["delivery_context_id"])
                if context["kind"] != "delivery":
                    raise ProvenanceValidationError(
                        "delivery association requires a delivery context"
                    )
            elif stage == 1:
                validator.name(value["slot"])
            elif stage == 2:
                validator.reference(value["state"], "state")
            elif stage == 3:
                observation = validator._delivery_observation(value["verification_observation_id"])
                if value["state"]["object_id"] != observation["state"]["object_id"]:
                    raise ProvenanceValidationError(
                        "delivery association and verification target differ"
                    )
            elif stage == 4:
                slot = (
                    value["delivery_context_id"],
                    canonical_document(_name_identity(value["slot"])),
                )
                if slot in validator.delivery_slots:
                    raise ProvenanceValidationError(
                        "duplicate delivery slot in one delivery instance"
                    )
                validator.delivery_slots.add(slot)
            else:
                self.stack.pop()
                return
        elif kind in {"reported_description", "observation"}:
            local_state = value["state"]["scope"] == "local"
            if stage == 0:
                validator.reference(value["state"], "state")
            elif stage == 1:
                if "source_context_id" in value:
                    validator.node(value["source_context_id"], "context")
            elif stage == 2:
                if "source_context_id" in value and local_state:
                    state = validator._description_node(value["state"]["object_id"], "state")
                    occurrence = validator._description_node(state["occurrence_id"], "occurrence")
                    known_context = occurrence.get("source_context_id")
                    if known_context and known_context != value["source_context_id"]:
                        raise ProvenanceValidationError(
                            "subject context differs from occurrence context"
                        )
            elif stage == 3:
                if "content" in value:
                    self._child(frame, "content", value["content"])
                    return
            elif stage == 4:
                coverage = value.get("coverage", [])
                if frame.cursor < len(coverage):
                    coverage_key = self.coverage_key(self.stack.identity(), coverage[frame.cursor])
                    if coverage_key in self.native_keys:
                        raise ProvenanceValidationError(
                            "duplicate profile-qualified coverage category"
                        )
                    self.native_keys.add(coverage_key)
                    self.stack.replace(replace(frame, cursor=frame.cursor + 1))
                    return
            elif stage == 5:
                profiles = value.get("profiles", [])
                if frame.cursor < len(profiles):
                    profile = profiles[frame.cursor]
                    if (
                        profile["profile"]["schema_id"]
                        == PROFILE + "/profiles/filesystem-observation.schema.json"
                    ):
                        if not local_state:
                            raise ProvenanceValidationError(
                                "filesystem profile requires a filesystem occurrence"
                            )
                        state = validator._description_node(value["state"]["object_id"], "state")
                        if (
                            validator._description_node(state["occurrence_id"])["kind"]
                            != "filesystem_object"
                        ):
                            raise ProvenanceValidationError(
                                "filesystem profile requires a filesystem occurrence"
                            )
                    self.stack.replace(replace(frame, cursor=frame.cursor + 1))
                    return
            elif kind == "observation":
                self._observation(frame, value)
                return
            else:
                self.stack.pop()
                return
        elif kind == "continuity":
            if stage < 2:
                key = "earlier_state" if stage == 0 else "later_state"
                validator.reference(value[key], "state")
            elif stage == 2:
                if value["earlier_state"]["object_id"] == value["later_state"]["object_id"]:
                    raise ProvenanceValidationError("continuity relates distinct state entities")
            else:
                self.stack.pop()
                return
        elif kind == "custody_assertion":
            if stage == 0:
                validator.reference(value["target"], "artifact", "occurrence")
            elif stage == 1:
                validator.node(value["custodian_agent_id"], "agent")
            elif stage == 2:
                if "context_id" in value:
                    validator.node(value["context_id"], "context")
            else:
                self.stack.pop()
                return
        elif kind == "availability_assertion":
            if stage == 0:
                validator.reference(value["occurrence"], "occurrence")
            elif stage in (1, 2):
                key, expected = (
                    ("context_id", "context") if stage == 1 else ("activity_id", "activity")
                )
                if key in value:
                    validator.node(value[key], expected)
            else:
                self.stack.pop()
                return
        elif kind == "journal_subject":
            if stage == 0:
                validator.reference(value["artifact"], "artifact")
            elif stage == 1:
                if validator.journal_id and value["journal_id"] != validator.journal_id:
                    raise ProvenanceValidationError(
                        "journal subject assertion names another journal"
                    )
            else:
                self.stack.pop()
                return
        elif kind == "extension":
            if stage == 0:
                validator.reference(value["subject"])
            elif stage == 1:
                self._child(frame, "typed", value["value"])
                return
            else:
                self.stack.pop()
                return
        else:
            raise RuntimeError("unknown semantic row operation")
        self._next(frame)

    def step(self) -> None:
        # Runtime import keeps the shared interpreter independent of construction
        # order while retaining the native timestamp and identity helpers.
        from .graph import temporal_scope, time_ns

        frame = self.stack.peek()
        if frame is None:
            raise RuntimeError("semantic program is complete")
        value = self.nodes.values[frame.node]
        validator, stage = self.validator, frame.stage
        operation = frame.operation
        if operation == "row":
            self._row(frame, value)
        elif operation in {"association", "activity_context"}:
            if stage == 0:
                key, expected = (
                    ("agent_id", "agent")
                    if operation == "association"
                    else ("context_id", "context")
                )
                validator.node(value[key], expected)
                self._next(frame)
            else:
                self.stack.pop()
        elif operation == "generic":
            if stage < 3:
                key, child = (
                    ("evidence", "evidence"),
                    ("identifiers", "identifier"),
                    ("profiles", "profile"),
                )[stage]
                self._items(frame, value[key] if key == "evidence" else value.get(key, []), child)
            elif stage == 3:
                items = value.get("metadata", [])
                if frame.cursor == len(items):
                    self._next(frame)
                else:
                    item = items[frame.cursor]
                    metadata_key = self.metadata_key(item)
                    if metadata_key in self.native_keys:
                        raise ProvenanceValidationError(
                            "duplicate source-qualified native metadata row"
                        )
                    self.native_keys.add(metadata_key)
                    self.stack.replace(replace(frame, cursor=frame.cursor + 1))
                    self.stack.push("native", self.nodes.identity(item))
            elif stage == 4:
                if "temporal_scope" in value:
                    temporal_scope(value["temporal_scope"])
                self._next(frame)
            elif stage == 5:
                if "at" in value:
                    time_ns(value["at"])
                self._next(frame)
            else:
                self.stack.pop()
        elif operation == "evidence":
            if stage == 0:
                validator.node(value["asserted_by_agent_id"], "agent")
                self._next(frame)
            elif stage == 1:
                if "source" in value:
                    validator.reference(value["source"])
                self._next(frame)
            elif stage == 2 and "source_identifier" in value:
                self._child(frame, "identifier", value["source_identifier"])
            elif stage == 2:
                self._next(frame)
            else:
                self.stack.pop()
        elif operation in {"identifier", "source"}:
            if stage == 0:
                validator.name(value["value" if operation == "identifier" else "field"])
                self._next(frame)
            elif stage == 1:
                key = "authority_id" if operation == "identifier" else "context_id"
                types = ("context", "agent") if operation == "identifier" else ("context",)
                if key in value:
                    validator.node(value[key], *types)
                self._next(frame)
            else:
                self.stack.pop()
        elif operation == "profile":
            if stage == 0:
                if not validator.catalog.validate_profile(value["profile"], value["data"]):
                    if validator.require_profiles:
                        raise UnresolvedContractError(
                            "exact metadata profile contract is unavailable"
                        )
                    validator.profiles[canonical_document(value["profile"])] = dict(
                        value["profile"]
                    )
                    self.stack.pop()
                elif value["profile"]["schema_id"] in {
                    PROFILE + "/profiles/" + name + ".schema.json"
                    for name in (
                        "filesystem-observation",
                        "filesystem-context",
                        "execution-environment",
                    )
                } and isinstance(value["data"], dict):
                    self._child(frame, "filesystem", value["data"])
                else:
                    self.stack.pop()
            else:
                self.stack.pop()
        elif operation == "filesystem":
            if stage < 4:
                key, child = (
                    ("identifiers", "identifier"),
                    ("native_identifiers", "identifier"),
                    ("principals", "principal"),
                    ("timestamps", "stamp"),
                )[stage]
                self._items(frame, value.get(key, []), child)
            elif stage == 4 and "posix_mode" in value:
                self._child(frame, "source", value["posix_mode"]["source"])
            elif stage == 4:
                self._next(frame)
            elif stage == 5:
                keys = ("operating_system", "kernel", "runtime", "host")
                if frame.cursor == len(keys):
                    self._next(frame)
                else:
                    key = keys[frame.cursor]
                    self.stack.replace(replace(frame, cursor=frame.cursor + 1))
                    if key in value and isinstance(value[key], dict):
                        self.stack.push("filesystem", self.nodes.identity(value[key]))
            else:
                self.stack.pop()
        elif operation == "principal":
            if stage == 0:
                self._child(frame, "identifier", value["identifier"])
            elif stage == 1:
                if "display_name" in value:
                    validator.name(value["display_name"])
                self._next(frame)
            elif stage == 2:
                self._child(frame, "source", value["source"])
            else:
                self.stack.pop()
        elif operation == "stamp":
            if stage == 0:
                self._child(frame, "source", value["source"])
            elif stage == 1:
                if "value" in value:
                    time_ns(value["value"])
                self._next(frame)
            elif stage == 2 and "raw" in value:
                self._child(frame, "typed", value["raw"]["value"])
            elif stage == 2:
                self._next(frame)
            elif stage == 3:
                if "raw" in value and "epoch" in value["raw"]:
                    time_ns(value["raw"]["epoch"])
                self._next(frame)
            else:
                self.stack.pop()
        elif operation == "native":
            if stage == 0:
                validator.name(value["name"])
                self._next(frame)
            elif stage == 1:
                self._child(frame, "source", value["source"])
            elif stage == 2 and "value" in value:
                self._child(frame, "typed", value["value"])
            elif stage == 2:
                self._next(frame)
            elif stage == 3:
                if (
                    "value" in value
                    and value["value"]["type"] in ("bytes", "digest")
                    and "observed_byte_length" in value
                ):
                    key = "byte_length" if value["value"]["type"] == "bytes" else "size_bytes"
                    if int(value["observed_byte_length"]) != int(value["value"]["value"][key]):
                        raise ProvenanceValidationError(
                            "metadata observed length differs from evidence"
                        )
                self._next(frame)
            elif stage == 4:
                self._items(frame, value.get("interpretations", []), "interpretation")
            else:
                self.stack.pop()
        elif operation == "interpretation":
            if stage == 0:
                validator.node(value["asserted_by_agent_id"], "agent")
                self._next(frame)
            elif stage == 1:
                self._child(frame, "typed", value["value"])
            else:
                self.stack.pop()
        elif operation == "content":
            if stage == 0:
                items = value["digests"]
                if frame.cursor == len(items):
                    self._next(frame)
                else:
                    digest_key = self.content_key(self.stack.identity(), items[frame.cursor])
                    if digest_key in self.native_keys:
                        raise ProvenanceValidationError("duplicate digest algorithm")
                    self.native_keys.add(digest_key)
                    self.stack.replace(replace(frame, cursor=frame.cursor + 1))
            else:
                self.stack.pop()
        elif operation == "typed":
            if stage == 0:
                kind, data = value["type"], value["value"]
                if kind == "json":
                    self._child(frame, "profile", data)
                    return
                if kind == "bytes":
                    validator.bytes(data)
                elif kind == "reference":
                    validator.reference(data)
                elif kind == "timestamp":
                    time_ns(data)
                elif kind == "digest":
                    self._child(frame, "content", data)
                    return
            self.stack.pop()
        else:
            raise RuntimeError("unknown semantic program operation")


def walk(validator: _Validator, operation: str, value: Any) -> None:
    if operation == "filesystem" and not isinstance(value, dict):
        return
    nodes = Nodes(value)
    stack = MemoryStack(operation)
    program = Program(validator, nodes, stack, set())
    while stack.peek() is not None:
        program.step()
