"""One explicit stack for native common assertion semantics.

Each transition visits one control frame, one reference, or one admitted local
atom. The stack and metadata-key set have interchangeable memory and SQLite
owners; neither traversal depends on replaying earlier transitions.
"""

from __future__ import annotations

from collections.abc import Mapping, MutableSet
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, Protocol

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

from .errors import ProvenanceValidationError, UnresolvedContractError

if TYPE_CHECKING:
    from .graph import _Validator


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

    def identity(self, value: Any) -> int:
        return self.identities[id(value)]


class Stack(Protocol):
    def peek(self) -> Frame | None: ...

    def replace(self, frame: Frame) -> None: ...

    def push(self, operation: str, node: int) -> None: ...

    def pop(self) -> None: ...


class MemoryStack:
    def __init__(self, operation: str) -> None:
        self.frames = [Frame(operation, 0)]

    def peek(self) -> Frame | None:
        return self.frames[-1] if self.frames else None

    def replace(self, frame: Frame) -> None:
        self.frames[-1] = frame

    def push(self, operation: str, node: int) -> None:
        self.frames.append(Frame(operation, node))

    def pop(self) -> None:
        self.frames.pop()


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
        if operation == "generic":
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
                    validator.content(data)
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
