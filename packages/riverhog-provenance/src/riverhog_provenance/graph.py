"""Executable application constraints for the formal, attributable assertion graph.

This validates declared semantics, not historical truth or cryptographic witness
trust. It does not pretend to implement every entailment of general PROV-Constraints.
"""

from __future__ import annotations

import base64
import binascii
import copy
import hashlib
import json
import re
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

from riverhog_provenance_contracts import (
    CATEGORY_TYPES,
    PROFILE,
    ContractCatalog,
    canonical_document,
)

from .common import fingerprint
from .constants import PRIMARY_CONTENT_CATEGORY
from .errors import ProvenanceValidationError, UnresolvedContractError

ENTITY_TYPES = frozenset(
    {"artifact", "occurrence", "state", "context", "observation", "reported_description"}
)
IDENTITY_TYPES = frozenset({"artifact", "occurrence", "state", "context", "agent"})


def time_ns(value: str) -> int:
    """Calendar-valid UTC comparison without microsecond truncation."""
    match = re.fullmatch(
        r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?Z",
        value,
    )
    if match is None:
        raise ProvenanceValidationError("noncanonical UTC instant")
    try:
        instant = datetime.strptime(match[1], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=UTC)
    except ValueError as exc:
        raise ProvenanceValidationError("invalid calendar instant") from exc
    delta = instant - datetime(1970, 1, 1, tzinfo=UTC)
    return (delta.days * 86400 + delta.seconds) * 1_000_000_000 + int(
        (match[2] or "").ljust(9, "0")
    )


def temporal_scope(value: Mapping[str, Any]) -> None:
    if value["kind"] == "instant":
        time_ns(value["at"])
    elif value["kind"] == "interval":
        for field in ("start", "end"):
            if field in value:
                time_ns(value[field])
        if "start" in value and "end" in value and time_ns(value["start"]) >= time_ns(value["end"]):
            raise ProvenanceValidationError("validity interval must have start < end")


def iter_assertions(graph: Mapping[str, Any]) -> Iterable[tuple[str, dict[str, Any]]]:
    for category in CATEGORY_TYPES:
        for item in graph.get(category, []):
            yield category, item


def graph_from_assertions(items: Iterable[tuple[str, Mapping[str, Any]]]) -> dict[str, Any]:
    graph: dict[str, Any] = {}
    for category, item in items:
        graph.setdefault(category, []).append(copy.deepcopy(dict(item)))
    # Arrays are semantic sets; this materialization has an explicit stable order.
    for rows in graph.values():
        rows.sort(key=lambda row: row["assertion_id"])
    return graph


def _name_identity(value: Mapping[str, Any]) -> dict[str, Any]:
    if value["kind"] == "text":
        return {"kind": "text", "text": value["text"]}
    return {"kind": "bytes", "encoding": value["encoding"], "bytes": value["bytes"]}


def _source_identity(value: Mapping[str, Any]) -> dict[str, Any]:
    return {**value, "field": _name_identity(value["field"])}


def _acyclic(edges: Iterable[tuple[str, str]], label: str) -> None:
    successors: dict[str, set[str]] = defaultdict(set)
    indegree: dict[str, int] = {}
    for source, target in edges:
        if source == target:
            raise ProvenanceValidationError(f"{label} cannot relate an entity to itself")
        indegree.setdefault(source, 0)
        indegree.setdefault(target, 0)
        if target not in successors[source]:
            successors[source].add(target)
            indegree[target] += 1
    ready = [node for node, degree in indegree.items() if degree == 0]
    consumed = 0
    while ready:
        current = ready.pop()
        consumed += 1
        for target in successors[current]:
            indegree[target] -= 1
            if indegree[target] == 0:
                ready.append(target)
    if consumed != len(indegree):
        raise ProvenanceValidationError(f"{label} graph contains a cycle")


@dataclass(frozen=True, slots=True)
class GraphValidation:
    """Isolated materialized view plus explicit unresolved dependencies/findings."""

    _graph_json: bytes
    _externals_json: bytes
    _profiles_json: bytes
    findings: tuple[str, ...] = ()

    @property
    def graph(self) -> dict[str, Any]:
        return cast(dict[str, Any], json.loads(self._graph_json))

    @property
    def external_references(self) -> tuple[dict[str, Any], ...]:
        return tuple(json.loads(self._externals_json))

    @property
    def unresolved_profiles(self) -> tuple[dict[str, str], ...]:
        return tuple(json.loads(self._profiles_json))

    @property
    def profiles_verified(self) -> bool:
        return not self.unresolved_profiles

    @property
    def objects(self) -> dict[str, dict[str, Any]]:
        return {row["id"]: row for _, row in iter_assertions(self.graph)}


class _Validator:
    def __init__(
        self,
        graph: Mapping[str, Any],
        *,
        catalog: ContractCatalog,
        journal_id: str | None,
        require_profiles: bool,
    ) -> None:
        self.graph = graph
        self.catalog = catalog
        self.journal_id = journal_id
        self.require_profiles = require_profiles
        self.objects: dict[str, dict[str, Any]] = {}
        self.assertions: set[str] = set()
        self.externals: dict[bytes, dict[str, Any]] = {}
        self.profiles: dict[bytes, dict[str, str]] = {}
        self.findings: list[str] = []
        for _, row in iter_assertions(graph):
            if row["id"] in self.objects:
                raise ProvenanceValidationError("duplicate effective entity/record identity")
            if row["assertion_id"] in self.assertions:
                raise ProvenanceValidationError("duplicate assertion identity")
            self.objects[row["id"]] = row
            self.assertions.add(row["assertion_id"])
        if self.assertions & self.objects.keys():
            raise ProvenanceValidationError(
                "assertion and entity identity domains must be distinct"
            )

    def node(self, identity: str, *types: str) -> dict[str, Any]:
        value = self.objects.get(identity)
        if value is None:
            raise ProvenanceValidationError(f"unresolved local identity: {identity}")
        if types and value["type"] not in types:
            raise ProvenanceValidationError(
                f"wrong reference type: {value['type']}; expected {types}"
            )
        return value

    def reference(self, value: Mapping[str, Any], *types: str) -> dict[str, Any] | None:
        if types and value["object_type"] not in types:
            raise ProvenanceValidationError("reference declares an incompatible object type")
        if value["scope"] == "local":
            return self.node(value["object_id"], value["object_type"])
        if value["journal_id"] == self.journal_id:
            raise ProvenanceValidationError(
                "an external reference cannot point into its own journal"
            )
        if value["object_id"] in self.objects:
            self.node(value["object_id"], value["object_type"])
        self.externals[canonical_document(value)] = dict(value)
        return None

    def profile(self, value: Mapping[str, Any]) -> None:
        if not self.catalog.validate_profile(value["profile"], value["data"]):
            if self.require_profiles:
                raise UnresolvedContractError("exact metadata profile contract is unavailable")
            self.profiles[canonical_document(value["profile"])] = dict(value["profile"])
            return
        schema_id = value["profile"]["schema_id"]
        if schema_id in {
            PROFILE + "/profiles/" + name + ".schema.json"
            for name in ("filesystem-observation", "filesystem-context", "execution-environment")
        }:
            self.filesystem_values(value["data"])

    def filesystem_values(self, value: Any) -> None:
        """Apply only declared built-in profile semantics, not arbitrary JSON keys."""
        if not isinstance(value, dict):
            return
        for identifier in value.get("identifiers", []):
            self.identifier(identifier)
        for identifier in value.get("native_identifiers", []):
            self.identifier(identifier)
        for principal in value.get("principals", []):
            self.identifier(principal["identifier"])
            if "display_name" in principal:
                self.name(principal["display_name"])
            self.source(principal["source"])
        for stamp in value.get("timestamps", []):
            self.source(stamp["source"])
            if "value" in stamp:
                time_ns(stamp["value"])
            if "raw" in stamp:
                self.typed(stamp["raw"]["value"])
                if "epoch" in stamp["raw"]:
                    time_ns(stamp["raw"]["epoch"])
        if "posix_mode" in value:
            self.source(value["posix_mode"]["source"])
        for key in ("operating_system", "kernel", "runtime", "host"):
            if key in value:
                self.filesystem_values(value[key])

    def source(self, value: Mapping[str, Any]) -> None:
        self.name(value["field"])
        if "context_id" in value:
            self.node(value["context_id"], "context")

    def bytes(self, value: Mapping[str, Any]) -> bytes:
        try:
            raw = base64.b64decode(value["data"], validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ProvenanceValidationError("invalid base64") from exc
        if base64.b64encode(raw).decode("ascii") != value["data"]:
            raise ProvenanceValidationError("noncanonical base64 padding bits")
        if len(raw) != int(value["byte_length"]):
            raise ProvenanceValidationError("native byte length does not match evidence")
        for item in value.get("digests", []):
            name = {"sha-256": "sha256", "sha-512": "sha512"}.get(item["algorithm"])
            if name and hashlib.new(name, raw).hexdigest() != item["value"]:
                raise ProvenanceValidationError("native evidence digest mismatch")
        return raw

    def name(self, value: Mapping[str, Any]) -> None:
        if value["kind"] == "bytes":
            self.bytes(value["bytes"])

    def identifier(self, value: Mapping[str, Any]) -> None:
        self.name(value["value"])
        if "authority_id" in value:
            self.node(value["authority_id"], "context", "agent")

    def content(self, value: Mapping[str, Any]) -> None:
        seen: set[tuple[str, str]] = set()
        for digest in value["digests"]:
            key = digest["algorithm"], digest.get("algorithm_uri", "")
            if key in seen:
                raise ProvenanceValidationError("duplicate digest algorithm")
            seen.add(key)

    def typed(self, value: Mapping[str, Any]) -> None:
        kind, data = value["type"], value["value"]
        if kind == "bytes":
            self.bytes(data)
        elif kind == "reference":
            self.reference(data)
        elif kind == "timestamp":
            time_ns(data)
        elif kind == "json":
            self.profile(data)
        elif kind == "digest":
            self.content(data)

    def native(self, value: Mapping[str, Any]) -> None:
        self.name(value["name"])
        self.source(value["source"])
        if "value" in value:
            self.typed(value["value"])
            content = value["value"]["value"]
            if value["value"]["type"] in ("bytes", "digest") and "observed_byte_length" in value:
                key = "byte_length" if value["value"]["type"] == "bytes" else "size_bytes"
                if int(value["observed_byte_length"]) != int(content[key]):
                    raise ProvenanceValidationError(
                        "metadata observed length differs from evidence"
                    )
        for item in value.get("interpretations", []):
            self.node(item["asserted_by_agent_id"], "agent")
            self.typed(item["value"])

    def generic(self, value: Mapping[str, Any]) -> None:
        for ev in value["evidence"]:
            self.node(ev["asserted_by_agent_id"], "agent")
            if "source" in ev:
                self.reference(ev["source"])
            if "source_identifier" in ev:
                self.identifier(ev["source_identifier"])
        for identifier in value.get("identifiers", []):
            self.identifier(identifier)
        for profile in value.get("profiles", []):
            self.profile(profile)
        native_keys: set[bytes] = set()
        for item in value.get("metadata", []):
            key = canonical_document(
                {
                    "profile_id": item["profile_id"],
                    "category": item["category"],
                    "namespace": item.get("namespace", ""),
                    "name": _name_identity(item["name"]),
                    "source": _source_identity(item["source"]),
                }
            )
            if key in native_keys:
                raise ProvenanceValidationError("duplicate source-qualified native metadata row")
            native_keys.add(key)
            self.native(item)
        if "temporal_scope" in value:
            temporal_scope(value["temporal_scope"])
        if "at" in value:
            time_ns(value["at"])

    def run(self) -> None:
        generation: dict[str, dict[str, Any]] = {}
        invalidation: dict[str, dict[str, Any]] = {}
        derivations: list[tuple[str, str]] = []
        specializations: list[tuple[str, str]] = []
        capture_targets: dict[str, str] = {}
        ends: set[str] = set()
        slots: set[tuple[str, bytes]] = set()
        for row in self.objects.values():
            self.generic(row)
            kind = row["type"]
            if kind == "occurrence":
                self.node(row["artifact_id"], "artifact")
                if "source_context_id" in row:
                    self.node(row["source_context_id"], "context")
            elif kind == "state":
                self.node(row["occurrence_id"], "occurrence")
                extent = row["extent"]
                if extent["kind"] == "segment":
                    self.typed(extent["start_boundary"])
                    self.typed(extent["end_boundary"])
            elif kind == "activity":
                for field in ("started_at", "ended_at"):
                    if field in row:
                        time_ns(row[field])
                if "started_at" in row and "ended_at" in row:
                    if time_ns(row["started_at"]) > time_ns(row["ended_at"]):
                        raise ProvenanceValidationError("activity ends before it starts")
                for association in row["associations"]:
                    self.node(association["agent_id"], "agent")
                for context in row.get("contexts", []):
                    self.node(context["context_id"], "context")
                for profile in row.get("details", []):
                    self.profile(profile)
                if "configuration" in row:
                    self.profile(row["configuration"])
                    actual = hashlib.sha256(
                        canonical_document(row["configuration"]["data"])
                    ).hexdigest()
                    if row["configuration_sha256"] != actual:
                        raise ProvenanceValidationError("capture configuration digest mismatch")
            elif kind in ("observation", "reported_description"):
                state = self.reference(row["state"], "state")
                if "source_context_id" in row:
                    self.node(row["source_context_id"], "context")
                    if state:
                        occurrence = self.node(state["occurrence_id"], "occurrence")
                        known_context = occurrence.get("source_context_id")
                        if known_context and known_context != row["source_context_id"]:
                            raise ProvenanceValidationError(
                                "subject context differs from occurrence context"
                            )
                if "content" in row:
                    self.content(row["content"])
                coverage = row.get("coverage", [])
                keys = {(item["profile_id"], item["category"]) for item in coverage}
                if len(keys) != len(coverage):
                    raise ProvenanceValidationError("duplicate profile-qualified coverage category")
                for profile in row.get("profiles", []):
                    if (
                        profile["profile"]["schema_id"]
                        == PROFILE + "/profiles/filesystem-observation.schema.json"
                    ):
                        if (
                            state is None
                            or self.node(state["occurrence_id"])["kind"] != "filesystem_object"
                        ):
                            raise ProvenanceValidationError(
                                "filesystem profile requires a filesystem occurrence"
                            )
                if kind == "observation":
                    if state is None or state["extent"]["kind"] == "unknown":
                        raise ProvenanceValidationError(
                            "direct observation requires a local bounded state"
                        )
                    capture = self.node(row["capture_id"], "activity")
                    if capture["kind"] != "observation":
                        raise ProvenanceValidationError(
                            "observation capture is not an observation activity"
                        )
                    if capture["outcome"] not in ("success", "partial"):
                        raise ProvenanceValidationError(
                            "a failed capture cannot establish complete primary fixity"
                        )
                    old = capture_targets.setdefault(capture["id"], state["id"])
                    if old != state["id"]:
                        raise ProvenanceValidationError(
                            "one capture cannot observe distinct states"
                        )
                    if not any(
                        c["profile_id"] == PROFILE
                        and c["category"] == PRIMARY_CONTENT_CATEGORY
                        and c["status"] == "complete"
                        for c in coverage
                    ):
                        raise ProvenanceValidationError(
                            "direct observation requires complete primary-content coverage"
                        )
                    incomplete = any(c["status"] in ("partial", "failed") for c in coverage)
                    if incomplete and capture["outcome"] != "partial":
                        raise ProvenanceValidationError(
                            "incomplete coverage requires partial capture outcome"
                        )
                    observers = {a["agent_id"] for a in capture["associations"]}
                    if not any(
                        ev["basis"] == "direct_measurement"
                        and ev["asserted_by_agent_id"] in observers
                        for ev in row["evidence"]
                    ):
                        raise ProvenanceValidationError(
                            "observation lacks attributed direct measurement evidence"
                        )
                    consistency = row["consistency"]
                    if "snapshot_identifier" in consistency:
                        self.identifier(consistency["snapshot_identifier"])
                    if (
                        consistency["level"] == "immutable_snapshot"
                        and not row["capabilities"]["stable_version_selection"]
                    ):
                        raise ProvenanceValidationError(
                            "snapshot claim requires stable-version capability"
                        )
                    if row.get("metadata") and not row["capabilities"]["native_metadata"]:
                        raise ProvenanceValidationError(
                            "native evidence contradicts source capability declaration"
                        )
                    self._check_metadata_coverage(row)
            elif kind in ("usage", "generation", "invalidation"):
                self.reference(row["state"], "state")
                activity = self.node(row["activity_id"], "activity")
                if kind == "generation":
                    if activity["kind"] == "observation":
                        raise ProvenanceValidationError(
                            "capture does not generate its observed state"
                        )
                    self._unique_lifecycle(generation, row, "generation")
                elif kind == "invalidation":
                    self._unique_lifecycle(invalidation, row, "invalidation")
                if "at" in row:
                    self._within_activity(row["at"], activity)
            elif kind == "derivation":
                self.reference(row["used_state"], "state")
                self.reference(row["generated_state"], "state")
                derivations.append(
                    (row["used_state"]["object_id"], row["generated_state"]["object_id"])
                )
                if "activity_id" in row:
                    self.node(row["activity_id"], "activity")
                    usage = self.node(row["usage_id"], "usage")
                    generated = self.node(row["generation_id"], "generation")
                    for relation, endpoint in (
                        (usage, "used_state"),
                        (generated, "generated_state"),
                    ):
                        if (
                            relation["activity_id"] != row["activity_id"]
                            or relation["state"]["object_id"] != row[endpoint]["object_id"]
                        ):
                            raise ProvenanceValidationError(
                                "qualified derivation does not match usage/generation"
                            )
            elif kind == "specialization":
                self.reference(row["specific"], *ENTITY_TYPES)
                self.reference(row["general"], *ENTITY_TYPES)
                specializations.append((row["specific"]["object_id"], row["general"]["object_id"]))
            elif kind == "continuity":
                self.reference(row["earlier_state"], "state")
                self.reference(row["later_state"], "state")
                if row["earlier_state"]["object_id"] == row["later_state"]["object_id"]:
                    raise ProvenanceValidationError("continuity relates distinct state entities")
            elif kind == "content_comparison":
                self._comparison(row)
            elif kind == "locator_binding":
                self._locator(row)
            elif kind == "locator_binding_end":
                binding = self.node(row["binding_id"], "locator_binding")
                if row["binding_id"] in ends:
                    raise ProvenanceValidationError(
                        "duplicate effective end for one locator binding"
                    )
                ends.add(row["binding_id"])
                scope = binding["temporal_scope"]
                bound = scope.get("at", scope.get("start"))
                if bound and time_ns(row["at"]) < time_ns(bound):
                    raise ProvenanceValidationError("binding end precedes the known binding")
            elif kind == "delivery_association":
                context = self.node(row["delivery_context_id"], "context")
                if context["kind"] != "delivery":
                    raise ProvenanceValidationError(
                        "delivery association requires a delivery context"
                    )
                self.name(row["slot"])
                self.reference(row["state"], "state")
                observation = self.node(row["verification_observation_id"], "observation")
                if row["state"]["object_id"] != observation["state"]["object_id"]:
                    raise ProvenanceValidationError(
                        "delivery association and verification target differ"
                    )
                slot = (context["id"], canonical_document(_name_identity(row["slot"])))
                if slot in slots:
                    raise ProvenanceValidationError(
                        "duplicate delivery slot in one delivery instance"
                    )
                slots.add(slot)
            elif kind == "custody_assertion":
                self.reference(row["target"], "artifact", "occurrence")
                self.node(row["custodian_agent_id"], "agent")
                if "context_id" in row:
                    self.node(row["context_id"], "context")
            elif kind == "availability_assertion":
                self.reference(row["occurrence"], "occurrence")
                if "context_id" in row:
                    self.node(row["context_id"], "context")
                if "activity_id" in row:
                    self.node(row["activity_id"], "activity")
            elif kind == "journal_subject":
                self.reference(row["artifact"], "artifact")
                if self.journal_id and row["journal_id"] != self.journal_id:
                    raise ProvenanceValidationError(
                        "journal subject assertion names another journal"
                    )
            elif kind == "extension":
                self.reference(row["subject"])
                self.typed(row["value"])
        _acyclic(derivations, "derivation")
        _acyclic(specializations, "specialization")
        self._lifecycle_times(generation, invalidation)
        self._address_completeness()
        self._evidence_conflicts()

    @staticmethod
    def _within_activity(at: str, activity: Mapping[str, Any]) -> None:
        if "started_at" in activity and time_ns(at) < time_ns(activity["started_at"]):
            raise ProvenanceValidationError("event precedes activity start")
        if "ended_at" in activity and time_ns(at) > time_ns(activity["ended_at"]):
            raise ProvenanceValidationError("event follows activity end")

    @staticmethod
    def _unique_lifecycle(index: dict[str, dict[str, Any]], row: dict[str, Any], name: str) -> None:
        key = row["state"]["object_id"]
        previous = index.get(key)
        if previous and (
            previous["activity_id"] != row["activity_id"] or previous.get("at") != row.get("at")
        ):
            raise ProvenanceValidationError(f"conflicting known {name} events for one state")
        index[key] = row

    def _lifecycle_times(
        self, generation: Mapping[str, Any], invalidation: Mapping[str, Any]
    ) -> None:
        for row in self.objects.values():
            if row["type"] not in ("usage", "invalidation") or "at" not in row:
                continue
            key = row["state"]["object_id"]
            generated = generation.get(key)
            if generated and "at" in generated and time_ns(row["at"]) < time_ns(generated["at"]):
                raise ProvenanceValidationError("state used or invalidated before known generation")
            ended = invalidation.get(key)
            if (
                row["type"] == "usage"
                and ended
                and "at" in ended
                and time_ns(row["at"]) > time_ns(ended["at"])
            ):
                raise ProvenanceValidationError("state used after known invalidation")

    def _check_metadata_coverage(self, observation: Mapping[str, Any]) -> None:
        rows = observation.get("metadata", [])
        coverage = observation.get("coverage", [])
        for row in rows:
            matches = [
                c
                for c in coverage
                if c["profile_id"] == row["profile_id"] and c["category"] == row["category"]
            ]
            if not matches:
                raise ProvenanceValidationError("native metadata has no declared coverage category")
            if row["status"] == "unreadable" and not any(
                c["status"] in ("partial", "failed") for c in matches
            ):
                raise ProvenanceValidationError(
                    "unreadable metadata requires partial/failed coverage"
                )
            if any(
                c["status"] in ("not_exposed", "not_applicable", "not_requested") for c in matches
            ):
                raise ProvenanceValidationError("metadata contradicts its declared coverage")

    def _comparison(self, row: Mapping[str, Any]) -> None:
        left = self.reference(row["left_description"], "observation", "reported_description")
        right = self.reference(row["right_description"], "observation", "reported_description")
        if left is None or right is None:
            if row["result"] != "indeterminate":
                self.findings.append(
                    f"{row['id']}: comparison depends on unavailable external content evidence"
                )
            return
        if "content" not in left or "content" not in right:
            if row["result"] != "indeterminate":
                raise ProvenanceValidationError(
                    "content comparison has no cited complete fixity evidence"
                )
            return
        same = fingerprint(left["content"]) == fingerprint(right["content"])
        if row["result"] == "matching_fixity" and not same:
            raise ProvenanceValidationError("matching-fixity comparison contradicts cited evidence")
        if row["result"] == "different" and same:
            raise ProvenanceValidationError(
                "different-content comparison contradicts cited SHA-256 evidence"
            )

    def _locator(self, row: Mapping[str, Any]) -> None:
        target = self.reference(row["target"], "state", "occurrence")
        context = self.node(row["context_id"], "context")
        locator = row["locator"]
        if locator["kind"] == "filesystem_path":
            if context["kind"] != "filesystem_namespace":
                raise ProvenanceValidationError(
                    "filesystem path requires a filesystem naming context"
                )
            self.name(locator["name"])
        elif locator["kind"] in ("object_key", "repository_id"):
            required_context = {"object_key": "object_service", "repository_id": "repository"}[
                locator["kind"]
            ]
            if context["kind"] != required_context:
                raise ProvenanceValidationError("typed locator has an incompatible naming context")
            key = "key" if locator["kind"] == "object_key" else "identifier"
            self.name(locator[key])
            if "version" in locator:
                self.name(locator["version"])
        elif locator["kind"] == "database_selector":
            if context["kind"] != "database":
                raise ProvenanceValidationError(
                    "database selector requires a database naming context"
                )
            self.profile(locator["selector"])
        elif locator["kind"] == "opaque":
            self.name(locator["value"])
        if "observation_id" in row:
            observation = self.node(row["observation_id"], "observation")
            state = self.node(observation["state"]["object_id"], "state")
            if target is None or target["id"] not in (state["id"], state["occurrence_id"]):
                raise ProvenanceValidationError(
                    "locator binding does not identify its observation subject"
                )
            if observation["address_status"] != "known":
                raise ProvenanceValidationError("observed locator contradicts address status")
            if row["temporal_scope"]["kind"] == "instant":
                capture = self.node(observation["capture_id"], "activity")
                self._within_activity(row["temporal_scope"]["at"], capture)

    def _address_completeness(self) -> None:
        linked = {
            r["observation_id"]
            for r in self.objects.values()
            if r["type"] == "locator_binding" and "observation_id" in r
        }
        for row in self.objects.values():
            if (
                row["type"] == "observation"
                and row["address_status"] == "known"
                and row["id"] not in linked
            ):
                raise ProvenanceValidationError(
                    "known address requires an observed locator binding"
                )

    def _evidence_conflicts(self) -> None:
        # Conflicting reports are evidence about a claim, not a reason to discard
        # that historical testimony. Findings never choose a latest or true report.
        by_state: dict[str, set[tuple[int, str]]] = defaultdict(set)
        for row in self.objects.values():
            if row["type"] in ("observation", "reported_description") and "content" in row:
                by_state[row["state"]["object_id"]].add(fingerprint(row["content"]))
        for state, claims in by_state.items():
            if len(claims) > 1:
                self.findings.append(
                    f"{state}: conflicting primary-content descriptions; no value selected"
                )


def validate_graph(
    graph: Mapping[str, Any],
    *,
    catalog: ContractCatalog | None = None,
    journal_id: str | None = None,
    require_profiles: bool = True,
) -> GraphValidation:
    """Validate a complete effective graph, not a fragment with unresolved locals."""
    selected = catalog or ContractCatalog()
    try:
        selected.validate(PROFILE + "/graph-fragment.schema.json", dict(graph))
        validator = _Validator(
            graph, catalog=selected, journal_id=journal_id, require_profiles=require_profiles
        )
        validator.run()
    except ProvenanceValidationError:
        raise
    except (ValueError, TypeError, KeyError, OverflowError) as exc:
        raise ProvenanceValidationError(str(exc)) from exc
    return GraphValidation(
        canonical_document(graph),
        json.dumps(list(validator.externals.values()), sort_keys=True).encode(),
        json.dumps(list(validator.profiles.values()), sort_keys=True).encode(),
        tuple(validator.findings),
    )
