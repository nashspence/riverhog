# SPDX-License-Identifier: Apache-2.0
"""Non-production, small-fixture model of annotation publication and composition.

This is not an archive reader, storage adapter, canonical journal implementation,
security boundary, or growth-qualified algorithm. It intentionally materializes
small snapshots and prefixes. Production must use the existing canonical owners,
append-stable storage and real source-binding proofs. Model formats use /v0.
"""
from __future__ import annotations

import copy
import hashlib
import json
import unicodedata
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

BASE = "e43df5b5acce5d730c28b48b084e87445fbaac3b"
CORE = "https://nashspence.github.io/riverhog/v1/provenance"
PROFILE = "urn:riverhog:reference:collection-annotations"
SCHEMA = PROFILE + ":activity"
CONTEXT_SCHEMA = PROFILE + ":context"
RECORDER = "urn:uuid:48a41e47-882d-55f8-bd67-b30d0ca3f316"
ACTOR = "urn:uuid:93673836-16e8-5b66-9096-5a314152aa51"
WHEN = "2026-10-13T14:00:00.000000001Z"


class Invalid(ValueError):
    """Selected evidence is incomplete or inconsistent in the model."""


class Conflict(Invalid):
    """A conditional transition cannot be accepted."""


def encoded(value: Any) -> bytes:
    """Fixture-only JCS subset: ASCII object keys, safe integers, no floats.

    This restricted spelling agrees with the owning encoder for these fixtures;
    it is not a replacement for the project's complete canonical JSON contract.
    """
    def check(v: Any) -> None:
        if isinstance(v, dict):
            if any(type(k) is not str or not k.isascii() for k in v):
                raise Invalid("fixture object keys must be ASCII")
            for child in v.values():
                check(child)
        elif isinstance(v, list):
            for child in v:
                check(child)
        elif type(v) is int:
            if abs(v) > 2**53 - 1:
                raise Invalid("fixture integer needs an exact string")
        elif type(v) not in (str, bool, type(None)):
            raise Invalid("unsupported fixture value")
    check(value)
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def identity(name: str) -> str:
    return uuid.uuid5(uuid.NAMESPACE_URL, PROFILE + ":" + name).urn


def assertion(record_type: str, name: str, **fields: Any) -> dict[str, Any]:
    return {"type": record_type, "id": identity(name), "assertion_id": identity(name + ":claim"),
            "evidence": [{"basis": "process_record", "asserted_by_agent_id": RECORDER}],
            **fields}


def agent(agent_id: str) -> dict[str, Any]:
    return {"type": "agent", "id": agent_id, "assertion_id": identity(agent_id + ":claim"),
            "kind": "software", "name": "Explicit reference-fixture software agent",
            "evidence": [{"basis": "identity_assignment", "asserted_by_agent_id": RECORDER}]}


def local(row: dict[str, Any]) -> dict[str, str]:
    return {"scope": "local", "object_id": row["id"], "object_type": row["type"]}


def parse_frame(raw: bytes) -> dict[str, Any]:
    if not raw.startswith(b"\x1e") or not raw.endswith(b"\n"):
        raise Invalid("invalid journal fixture framing")
    value = json.loads(raw[1:-1])
    if b"\x1e" + encoded(value) + b"\n" != raw:
        raise Invalid("noncanonical fixture frame")
    return value


def entry_reference(raw: bytes) -> dict[str, Any]:
    row = parse_frame(raw)
    return {"entry_id": row["id"], "sequence": row["sequence"],
            "json_sha256": digest(raw[1:-1])}


def external(raw: bytes, row: dict[str, Any]) -> dict[str, Any]:
    return {"scope": "external", "journal_id": parse_frame(raw)["journal_id"],
            "entry": entry_reference(raw), "assertion_id": row["assertion_id"],
            "object_id": row["id"], "object_type": row["type"]}


def journal_frame(journal_id: str, name: str, groups: dict[str, Any],
                  when: str, previous: bytes | None = None) -> bytes:
    sequence = 0 if previous is None else int(parse_frame(previous)["sequence"]) + 1
    body: dict[str, Any] = {"assertions": groups}
    if previous is None:
        body["journal"] = {"id": journal_id, "serialization": "application/json-seq",
                           "entry_encoding": "RFC8785", "hash_algorithm": "sha-256",
                           "writer_policy": "single_writer", "policy_uri": PROFILE}
    result: dict[str, Any] = {
        "$schema": CORE + "/journal-entry.schema.json", "profile": CORE,
        "schema_version": "1.0.0", "type": "riverhog_provenance_journal_entry",
        "id": identity(name + ":entry"), "journal_id": journal_id,
        "sequence": str(sequence), "recorded_at": when,
        "recorded_by_agent_id": RECORDER,
        "entry_kind": "journal_init" if previous is None else "assertion", "body": body,
    }
    if previous is not None:
        result["previous_entry"] = entry_reference(previous)
    return b"\x1e" + encoded(result) + b"\n"


def profile_reference(schema: str) -> dict[str, str]:
    # Match the existing binding envelope using this fixture's restricted encoder.
    pack = json.loads(Path(__file__).with_name("profile.schema.json").read_bytes())
    raw = encoded({"format": "riverhog-provenance-contract-binding/v1",
                   "contract_id": PROFILE,
                   "schema_dialect": "https://json-schema.org/draft/2020-12/schema",
                   "format_policy": "annotation-only",
                   "schemas": {s["$id"]: s for s in pack["schemas"]}})
    return {"contract_id": PROFILE, "contract_sha256": digest(raw), "schema_id": schema}


def checked_value(action: str, value: str | None) -> None:
    if action not in {"tag_add", "tag_remove", "description_set", "description_clear"}:
        raise Invalid("unknown annotation operation")
    if action == "description_clear":
        if value is not None:
            raise Invalid("clear has no value")
        return
    if not isinstance(value, str) or not value.strip() or unicodedata.normalize("NFC", value) != value:
        raise Invalid("annotation value must be nonblank exact NFC")
    if any(0xD800 <= ord(c) <= 0xDFFF or c == "\0" for c in value):
        raise Invalid("invalid Unicode scalar")
    if action.startswith("tag_") and (value != value.strip() or any(ord(c) < 32 for c in value)):
        raise Invalid("invalid tag")
    limit = 65536 if action.startswith("tag_") else 32768
    if len(value.encode("utf-8")) > limit:
        raise Invalid("individual value exceeds its retained acceptance limit")


def apply(state: dict[str, Any], action: str, value: str | None) -> dict[str, Any]:
    checked_value(action, value)
    result = copy.deepcopy(state)
    tags = set(state["tags"])
    if action == "tag_add":
        tags.add(value)
    elif action == "tag_remove":
        tags.discard(value)
    elif action == "description_set":
        result["description"] = {"status": "present", "value": value}
    else:
        result["description"] = {"status": "unset"}
    result["tags"] = sorted(tags, key=lambda x: x.encode("utf-8"))
    return result


@dataclass
class ArchiveFixture:
    """Small fixture proof, NOT Riverhog's real SourceBindingProof format."""
    proof: bytes
    original_journal: bytes
    member_id: str
    state: dict[str, Any]

    @property
    def root(self) -> str:
        return digest(self.proof)

    @classmethod
    def from_repository(cls, repo: Path) -> ArchiveFixture:
        raw = (repo / "packages/riverhog-provenance/examples/never-named-emission.jsonseq").read_bytes()
        state = parse_frame(raw)["body"]["assertions"]["states"][0]
        member = "a" * 64
        # Duplicate-byte members are deliberately different archival memberships.
        rows = [{"member_id": m, "journal_sha256": digest(raw), "state": external(raw, state)}
                for m in (member, "b" * 64)]
        proof = encoded({"format": "reference-membership-fixture/v0", "members": rows})
        return cls(proof, raw, member, state)


@dataclass
class Prepared:
    predecessor: str | None
    checkpoint: str
    objects: dict[str, bytes]
    no_change: bool = False


@dataclass
class Recovered:
    checkpoint: str
    state: dict[str, Any]
    frames: list[bytes]
    checkpoints: list[str]
    required: set[str]

    @property
    def journal(self) -> bytes:
        return b"".join(self.frames)


@dataclass
class Copy:
    root: str
    objects: dict[str, bytes] = field(default_factory=dict)
    head: str | None = None
    catalog_head: str | None = None

    def read(self, key: str) -> bytes:
        raw = self.objects.get(key)
        if raw is None or digest(raw) != key:
            raise Invalid("missing or changed selected object")
        return raw

    def recover(self, checkpoint: str | None = None) -> Recovered:
        selected = checkpoint or self.head
        if selected is None:
            raise Invalid("missing selector is not empty history")
        stack: list[tuple[str, dict[str, Any]]] = []
        required: set[str] = set()
        key: str | None = selected
        while key is not None:
            if key in required:
                raise Invalid("cyclic checkpoint ancestry")
            required.add(key)
            row = json.loads(self.read(key))
            if row["root"] != self.root:
                raise Invalid("checkpoint belongs to another collection")
            stack.append((key, row))
            key = row.get("predecessor")
        frames: list[bytes] = []
        states: list[dict[str, Any]] = []
        operations: set[str] = set()
        stream: str | None = None
        for ordinal, (_, checkpoint_row) in enumerate(reversed(stack)):
            raw = self.read(checkpoint_row["entry"])
            snap_raw = self.read(checkpoint_row["snapshot"])
            required.update((checkpoint_row["entry"], checkpoint_row["snapshot"]))
            snap = json.loads(snap_raw)
            event = parse_frame(raw)
            if event["sequence"] != str(ordinal) or checkpoint_row["revision"] != str(ordinal):
                raise Invalid("history revision gap")
            if ordinal == 0:
                stream = event["journal_id"]
                context = event["body"]["assertions"]["contexts"][0]
                if context["kind"] != "recordkeeping" or context["profiles"][0]["data"]["archive_root_sha256"] != self.root:
                    raise Invalid("wrong annotation context")
            elif event.get("previous_entry") != entry_reference(frames[-1]):
                raise Invalid("journal predecessor differs")
            if event["journal_id"] != stream:
                raise Invalid("mixed annotation streams")
            activity = event["body"]["assertions"]["activities"][0]
            detail = activity["details"][0]["data"]
            if detail["archive_root_sha256"] != self.root or detail["revision"] != str(ordinal):
                raise Invalid("wrong activity subject or revision")
            if detail["snapshot_sha256"] != checkpoint_row["snapshot"]:
                raise Invalid("documentary and selected state differ")
            if detail.get("predecessor") != checkpoint_row.get("predecessor"):
                raise Invalid("documentary and selected predecessor differ")
            if detail["operation_id"] in operations:
                raise Invalid("duplicate logical operation")
            operations.add(detail["operation_id"])
            if ordinal == 0:
                if detail["operation"] != "initial":
                    raise Invalid("history has no initial establishment")
                for tag in snap["tags"]:
                    checked_value("tag_add", tag)
                if snap["description"]["status"] == "present":
                    checked_value("description_set", snap["description"]["value"])
            else:
                expected = apply(states[-1], detail["operation"], detail.get("value"))
                if expected != snap or expected == states[-1]:
                    raise Invalid("history replay disagrees or invents a no-op")
            frames.append(raw)
            states.append(snap)
            anchor = {"journal_id": stream, "through": entry_reference(raw),
                      "prefix_sha256": digest(b"".join(frames)),
                      "prefix_bytes": str(sum(map(len, frames)))}
            if checkpoint_row["journal"] != anchor:
                raise Invalid("selected prefix differs")
        return Recovered(selected, states[-1], frames, [r[0] for r in reversed(stack)], required)

    def prepare(self, operation_id: str, operation: str, value: str | None = None,
                *, expected: str | None, when: str = WHEN,
                initial_tags: tuple[str, ...] = ()) -> Prepared:
        if not operation_id:
            raise Invalid("operation identity is required")
        request = {"operation_id": operation_id, "operation": operation,
                   "value": value, "expected": expected, "actor": ACTOR,
                   "initial_tags": list(initial_tags)}
        request_id = digest(encoded(request))
        old = self.recover() if self.head else None
        if old:
            for checkpoint, raw in zip(old.checkpoints, old.frames, strict=True):
                d = parse_frame(raw)["body"]["assertions"]["activities"][0]["details"][0]["data"]
                if d["operation_id"] == operation_id:
                    if d["request_sha256"] != request_id:
                        raise Conflict("operation identity reused for different input")
                    return Prepared(expected, checkpoint, {}, True)
        if expected != self.head:
            raise Conflict("stale common predecessor")
        previous = old.frames[-1] if old else None
        if old is None:
            if operation != "initial":
                raise Invalid("initial establishment required")
            state = {"tags": sorted(set(initial_tags), key=lambda t: t.encode("utf-8")),
                     "description": {"status": "unset"}}
            for tag in state["tags"]:
                checked_value("tag_add", tag)
        else:
            state = apply(old.state, operation, value)
            if state == old.state:
                return Prepared(expected, old.checkpoint, {}, True)
        revision = len(old.frames) if old else 0
        snap = encoded(state)
        data = {"archive_root_sha256": self.root, "revision": str(revision),
                "operation_id": operation_id, "operation": operation,
                "request_sha256": request_id, "snapshot_sha256": digest(snap)}
        if expected:
            data["predecessor"] = expected
        if value is not None:
            data["value"] = value
        context = assertion("context", self.root + ":context", kind="recordkeeping",
                            profiles=[{"profile": profile_reference(CONTEXT_SCHEMA),
                                       "data": {"archive_root_sha256": self.root}}])
        name = self.root + ":" + operation_id
        activity = assertion("activity", name, kind="metadata_update", outcome="success",
                             associations=[{"agent_id": ACTOR, "role": PROFILE + ":initiator"}],
                             contexts=[{"context_id": context["id"], "role": "recording"}],
                             details=[{"profile": profile_reference(SCHEMA), "data": data}])
        groups: dict[str, Any] = {"activities": [activity]}
        if old is None:
            groups.update(agents=[agent(RECORDER), agent(ACTOR)], contexts=[context])
        raw = journal_frame(identity(self.root + ":journal"), name, groups, when, previous)
        journal = (old.journal if old else b"") + raw
        checkpoint_row = {
            "format": "reference-annotation-checkpoint/v0", "root": self.root,
            "revision": str(revision), "entry": digest(raw), "snapshot": digest(snap),
            "journal": {"journal_id": parse_frame(raw)["journal_id"],
                        "through": entry_reference(raw), "prefix_sha256": digest(journal),
                        "prefix_bytes": str(len(journal))},
        }
        if expected:
            checkpoint_row["predecessor"] = expected
        checkpoint_bytes = encoded(checkpoint_row)
        objects = {digest(v): v for v in (snap, raw, checkpoint_bytes)}
        return Prepared(expected, digest(checkpoint_bytes), objects)

    def stage(self, prepared: Prepared) -> None:
        for key, raw in prepared.objects.items():
            if digest(raw) != key or (key in self.objects and self.objects[key] != raw):
                raise Invalid("immutable object mismatch")
            self.objects[key] = raw

    def publish(self, prepared: Prepared) -> str:
        # Model of a provider-conditional selector transition, not a real CAS.
        self.recover(prepared.checkpoint)
        if self.head and prepared.checkpoint in self.recover().checkpoints:
            return prepared.checkpoint
        if self.head != prepared.predecessor:
            raise Conflict("stale publication attempt")
        self.head = prepared.checkpoint
        return self.head

    def mutate(self, operation_id: str, operation: str, value: str | None = None,
               **kwargs: Any) -> str:
        prepared = self.prepare(operation_id, operation, value, expected=self.head, **kwargs)
        self.stage(prepared)
        return self.publish(prepared)

    def reconcile_catalog(self) -> None:
        self.catalog_head = self.recover().checkpoint

    def collect(self, pending: tuple[Prepared, ...] = ()) -> None:
        live = self.recover().required if self.head else set()
        for attempt in pending:
            live.update(attempt.objects)
            if attempt.predecessor:
                live.update(self.recover(attempt.predecessor).required)
        self.objects = {k: v for k, v in self.objects.items() if k in live}


def newest_compatible(copy_a: Copy, copy_b: Copy) -> str:
    if copy_a.root != copy_b.root:
        raise Conflict("different collections")
    a, b = copy_a.recover(), copy_b.recover()
    if a.checkpoint in b.checkpoints:
        return b.checkpoint
    if b.checkpoint in a.checkpoints:
        return a.checkpoint
    raise Conflict("divergent ancestry is not a timestamp/revision contest")


def compose(source: Copy, fixture: ArchiveFixture, member_id: str,
            *, checkpoint: str, when: str = WHEN) -> dict[str, Any]:
    if fixture.root != source.root:
        raise Invalid("wrong collection membership proof")
    selected = source.recover(checkpoint)
    proof = json.loads(fixture.proof)
    members = [m for m in proof["members"] if m["member_id"] == member_id]
    if len(members) != 1 or members[0]["journal_sha256"] != digest(fixture.original_journal):
        raise Invalid("missing or mismatched member proof")
    state_ref = external(fixture.original_journal, fixture.state)
    if members[0]["state"] != state_ref:
        raise Invalid("member State differs from original evidence")
    context = parse_frame(selected.frames[0])["body"]["assertions"]["contexts"][0]
    context_ref = external(selected.frames[0], context)
    name = fixture.root + ":" + member_id + ":" + checkpoint
    bridge = assertion("extension", name + ":membership", subject=state_ref,
                       property=PROFILE + ":archived-member-context",
                       value={"type": "reference", "value": context_ref})
    # Only the bridge is new; neither original journal is rewritten.
    bridge_raw = journal_frame(identity(name + ":composition"), name,
                               {"agents": [agent(RECORDER)], "extensions": [bridge]}, when)
    objects = {k: source.read(k) for k in selected.required}
    for raw in (fixture.proof, fixture.original_journal, bridge_raw):
        objects[digest(raw)] = raw
    return {"format": "reference-portable-member-context/v0", "root": fixture.root,
            "member_id": member_id, "checkpoint": checkpoint,
            "original_journal": digest(fixture.original_journal), "bridge": digest(bridge_raw),
            "objects": objects}


def verify_portable(bundle: dict[str, Any]) -> Recovered:
    """Verify this fixture package with no source Copy or repository access."""
    objects = bundle["objects"]
    for key, raw in objects.items():
        if digest(raw) != key:
            raise Invalid("portable object changed")
    source = Copy(bundle["root"], objects, bundle["checkpoint"])
    selected = source.recover()
    try:
        proof_raw = source.read(bundle["root"])
        proof = json.loads(proof_raw)
        rows = [r for r in proof["members"] if r["member_id"] == bundle["member_id"]]
        original = source.read(bundle["original_journal"])
        state = parse_frame(original)["body"]["assertions"]["states"][0]
        if len(rows) != 1 or rows[0]["journal_sha256"] != digest(original) or rows[0]["state"] != external(original, state):
            raise Invalid("portable membership differs")
        bridge = parse_frame(source.read(bundle["bridge"]))["body"]["assertions"]["extensions"][0]
        context = parse_frame(selected.frames[0])["body"]["assertions"]["contexts"][0]
        if bridge["subject"] != external(original, state) or bridge["value"] != {"type": "reference", "value": external(selected.frames[0], context)}:
            raise Invalid("composition changed its referent")
        if bridge["property"] != PROFILE + ":archived-member-context":
            raise Invalid("composition changed the relation")
    except (KeyError, IndexError, TypeError) as exc:
        raise Invalid("portable closure is incomplete") from exc
    return selected
