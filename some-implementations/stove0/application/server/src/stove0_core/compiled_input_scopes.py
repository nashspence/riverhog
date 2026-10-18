"""Resolve compiled subject ports through resumable exact selection builders."""

from __future__ import annotations

import json

from pydantic import JsonValue
from stove0_protocol import ArtifactSelectionRef, WorkIdentity, canonical_json_sha256
from stove0_recipe_config.compiled import CompiledRoleSelection

from stove0_core.compiled_state_ports import CompiledStatePort


class CompiledInputScopes:
    def __init__(self, state: CompiledStatePort) -> None:
        self.state = state

    def subject_port(
        self,
        *,
        work: WorkIdentity,
        task_id: str,
        port_id: str,
        binding: str | CompiledRoleSelection,
        inventory: ArtifactSelectionRef,
    ) -> ArtifactSelectionRef | None:
        if self.state.compiled_planning.inventory_ref(work.work_id) != inventory:
            raise ValueError("subject port changed the exact original invocation scope")
        if binding == "all":
            return inventory
        if not isinstance(binding, CompiledRoleSelection):
            raise ValueError("subject port requires compiled all/role selection")
        identity: dict[str, JsonValue] = {
            "format": "stove0-subject-port-selection-builder/v1",
            "work_id": work.work_id,
            "task_id": task_id,
            "port_id": port_id,
            "roles": list(binding.roles),
            "inventory": inventory.model_dump(mode="json"),
        }
        key = canonical_json_sha256(identity)
        builders = self.state.metadata_selections
        row = builders.ensure(key, binding=identity, source={"after_subject_id": ""})
        if row["state"] != "collecting":
            return builders.seal_step(key)
        source = json.loads(row["source_json"])
        page, cursor, complete = self.state.compiled_planning.role_page(
            work.work_id,
            roles=binding.roles,
            after_subject_id=source["after_subject_id"],
            limit=100,
        )
        builders.append(
            key,
            expected_revision=row["revision"],
            subjects=page,
            source={"after_subject_id": cursor or source["after_subject_id"]},
            complete=complete,
        )
        return None

    def subject_union(
        self, *, work: WorkIdentity, task_id: str, ports: dict[str, ArtifactSelectionRef]
    ) -> ArtifactSelectionRef | None:
        if not ports:
            raise ValueError("observation question requires declared subject ports")
        if len(ports) == 1:
            return next(iter(ports.values()))
        identity: dict[str, JsonValue] = {
            "format": "stove0-subject-port-union-builder/v1",
            "work_id": work.work_id,
            "task_id": task_id,
            "ports": {
                name: reference.model_dump(mode="json") for name, reference in sorted(ports.items())
            },
        }
        key = canonical_json_sha256(identity)
        builders = self.state.metadata_selections
        row = builders.ensure(
            key, binding=identity, source={"port_ordinal": 0, "continuation": None}
        )
        if row["state"] != "collecting":
            return builders.seal_step(key)
        source = json.loads(row["source_json"])
        names = sorted(ports)
        port_ordinal = source["port_ordinal"]
        if port_ordinal >= len(names):
            raise ValueError("subject union continuation exceeds its declared ports")
        reference = ports[names[port_ordinal]]
        if self.state.load_selection_ref(reference.selection_sha256) != reference:
            raise ValueError("subject union source selection is unavailable")
        page, cursor, complete = self.state.selection_artifact_page(
            reference.selection_sha256, continuation=source["continuation"], limit=100
        )
        updated = (
            {"port_ordinal": port_ordinal + 1, "continuation": None}
            if complete
            else {"port_ordinal": port_ordinal, "continuation": cursor}
        )
        # Builder membership rejects overlap rather than silently deduplicating
        # role ports that the selected interface requires to be disjoint.
        builders.append(
            key,
            expected_revision=row["revision"],
            subjects=page,
            source=updated,
            complete=complete and port_ordinal + 1 == len(names),
        )
        return None
