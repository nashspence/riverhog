"""Actual materialization-owner observations and a disposable trie proposal.

This reference does not install a durable checkpoint or change product code. The
trie delegates validation/escaping to the current materialization owner and
changes only collision indexing. All database paths supplied here are disposable
experiment scratch. The recorder counts actual calls/encoded parameters, not
physical fsyncs or a maintained performance profile.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import stat
import time
from collections.abc import Iterable, Iterator
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import riverhog_materialization as owner
from a_riverhog_cli.local import _publish_file
from riverhog_canonical_json import canonical_json_bytes, require_canonical_json
from riverhog_client._file_download import verified_download
from riverhog_materialization import (
    DestinationRules,
    FallbackReason,
    MaterializationMode,
    MemberAdvice,
    PlannedDestination,
    id_components,
    primary_sidecar_components,
)
from riverhog_protocol.artifact_identity import ArtifactId
from riverhog_provenance_contracts import ContractCatalog


class ObservedConnection(sqlite3.Connection):
    """Count the actual SQL owner; do not pretend these are VFS writes."""

    sql_calls: int = 0
    commit_calls: int = 0
    prefix_rows: int = 0
    prefix_parameter_bytes: int = 0

    def execute(self, sql: str, parameters: Any = (), /) -> sqlite3.Cursor:
        self.sql_calls += 1
        if sql.lstrip().startswith("INSERT INTO materialization_prefixes VALUES"):
            self.prefix_rows += 1
            self.prefix_parameter_bytes += len(parameters[0]) + len(parameters[1])
        return super().execute(sql, parameters)

    def commit(self) -> None:
        self.commit_calls += 1
        super().commit()


def _components(raw: bytes) -> tuple[str, ...]:
    value = require_canonical_json(raw)
    if not isinstance(value, list) or any(type(part) is not str for part in value):
        raise ValueError("materialization plan state has invalid components")
    return tuple(cast(list[str], value))


def plan_trie_spooled(
    members: Iterable[MemberAdvice],
    *,
    rules: DestinationRules,
    state: sqlite3.Connection,
    mode: MaterializationMode = "declared-hints",
) -> Iterator[PlannedDestination]:
    """Proposed parent-ID collision index, entirely disposable and owner local.

    Reserve this separate scratch connection before calling; never pass the CLI's
    mandatory local-state connection. Children are created after their parents.
    Consequently one ascending node pass propagates conflicts without revisiting
    whole ancestor paths or walking each conflicting subtree separately.
    """
    if mode not in ("declared-hints", "id-layout"):
        raise ValueError("unknown materialization mode")
    catalog = ContractCatalog()
    state.executescript(
        """
        CREATE TABLE trie_nodes (
            node_id INTEGER PRIMARY KEY,
            parent INTEGER NOT NULL,
            normalized TEXT NOT NULL,
            spelling TEXT NOT NULL,
            spelling_conflict INTEGER NOT NULL DEFAULT 0,
            leaf_count INTEGER NOT NULL DEFAULT 0,
            child_count INTEGER NOT NULL DEFAULT 0,
            blocked INTEGER NOT NULL DEFAULT 0,
            UNIQUE(parent, normalized)
        );
        INSERT INTO trie_nodes(node_id, parent, normalized, spelling)
            VALUES (0, -1, '', '');
        CREATE TABLE trie_members (
            artifact_id TEXT PRIMARY KEY,
            components BLOB NOT NULL,
            hint BLOB,
            reason TEXT NOT NULL,
            leaf_node INTEGER
        );
        """
    )
    for member in members:
        artifact_id = str(ArtifactId(member.artifact_id))
        hint = None
        if member.materialization_hint is not None:
            # Source-owned private functions are reused by this pinned reference;
            # production places the algorithm in that same owning module.
            catalog.validate(owner._HINT_SCHEMA, member.materialization_hint)
            hint = tuple(member.materialization_hint["components"])
        components: tuple[str, ...] = id_components(artifact_id)
        reason: FallbackReason = "id-layout" if mode == "id-layout" else "no-hint"
        if mode == "declared-hints" and hint is not None:
            candidate = (
                "files",
                *(owner._escape_validated_component(value, rules) for value in hint),
            )
            if rules.fits(candidate):
                components = candidate
                reason = "hint" if candidate[1:] == hint else "escaped-hint"
            else:
                reason = "destination-limits"
        if not rules.fits(id_components(artifact_id)) or not rules.fits(
            primary_sidecar_components(artifact_id)
        ):
            raise ValueError("destination cannot represent mandatory ID and provenance layout")
        leaf = None
        if components[0] == "files":
            parent = 0
            for spelling in components:
                normalized = rules.equivalent(spelling)
                row = state.execute(
                    "SELECT node_id, spelling FROM trie_nodes WHERE parent = ? AND normalized = ?",
                    (parent, normalized),
                ).fetchone()
                if row is None:
                    cursor = state.execute(
                        "INSERT INTO trie_nodes(parent, normalized, spelling) VALUES (?, ?, ?)",
                        (parent, normalized, spelling),
                    )
                    assert cursor.lastrowid is not None
                    node = int(cursor.lastrowid)
                    state.execute(
                        "UPDATE trie_nodes SET child_count = child_count + 1 WHERE node_id = ?",
                        (parent,),
                    )
                else:
                    node = int(row[0])
                    if row[1] != spelling:
                        state.execute(
                            "UPDATE trie_nodes SET spelling_conflict = 1 WHERE node_id = ?",
                            (node,),
                        )
                parent = node
            leaf = parent
            state.execute(
                "UPDATE trie_nodes SET leaf_count = leaf_count + 1 WHERE node_id = ?", (leaf,)
            )
        try:
            state.execute(
                "INSERT INTO trie_members VALUES (?, ?, ?, ?, ?)",
                (
                    artifact_id,
                    canonical_json_bytes(list(components)),
                    canonical_json_bytes(list(hint)) if hint is not None else None,
                    reason,
                    leaf,
                ),
            )
        except sqlite3.IntegrityError as exc:
            raise ValueError("artifact selection contains a duplicate member") from exc
    for node, parent, spelling_conflict, leaves, children in state.execute(
        "SELECT node_id, parent, spelling_conflict, leaf_count, child_count "
        "FROM trie_nodes WHERE node_id > 0 ORDER BY node_id"
    ):
        parent_blocked = state.execute(
            "SELECT blocked FROM trie_nodes WHERE node_id = ?", (parent,)
        ).fetchone()[0]
        blocked = parent_blocked or spelling_conflict or leaves > 1 or (leaves and children)
        state.execute(
            "UPDATE trie_nodes SET blocked = ? WHERE node_id = ?", (int(bool(blocked)), node)
        )
    for artifact_id, raw_components, raw_hint, reason, blocked in state.execute(
        "SELECT artifact_id, components, hint, reason, COALESCE(blocked, 0) "
        "FROM trie_members LEFT JOIN trie_nodes ON leaf_node = node_id ORDER BY artifact_id"
    ):
        yield PlannedDestination(
            artifact_id,
            id_components(artifact_id) if blocked else _components(raw_components),
            primary_sidecar_components(artifact_id),
            "destination-collision" if blocked else reason,
            _components(raw_hint) if raw_hint is not None else None,
        )


@dataclass(frozen=True)
class PlannerObservation:
    implementation: str
    members: int
    hint_depth: int
    input_hint_bytes: int
    total_hint_components: int
    index_rows: int
    index_key_spelling_bytes: int
    sql_execute_calls: int
    explicit_commit_calls: int
    backing_bytes: int
    elapsed_ns: int
    result_sha256: str


def observe_planner(directory: Path, *, members: int, depth: int, trie: bool) -> PlannerObservation:
    directory.mkdir(parents=True, exist_ok=False)
    database = directory / "plan.sqlite3"
    state = sqlite3.connect(database, factory=ObservedConnection)
    assert isinstance(state, ObservedConnection)
    # Both observations use disposable scratch; these settings do not qualify
    # the mandatory local materialization state database or final filesystem.
    state.execute("PRAGMA journal_mode=OFF")
    state.execute("PRAGMA synchronous=OFF")
    advice = [
        MemberAdvice(
            hashlib.sha256(f"member-{number}".encode()).hexdigest(),
            {"components": [*(f"d{index:03}" for index in range(depth - 1)), f"f{number:06}"]},
        )
        for number in range(members)
    ]
    rules = DestinationRules(False, True, "exact", 255, 4096)
    start = time.perf_counter_ns()
    try:
        function = plan_trie_spooled if trie else owner.plan_materialization_spooled
        rows = tuple(function(advice, rules=rules, state=state))
        elapsed = time.perf_counter_ns() - start
        if trie:
            count, key_bytes = state.execute(
                "SELECT COUNT(*), SUM(length(CAST(normalized AS BLOB)) + "
                "length(CAST(spelling AS BLOB))) FROM trie_nodes WHERE node_id > 0"
            ).fetchone()
        else:
            count, key_bytes = state.execute(
                "SELECT COUNT(*), SUM(length(key) + length(spelling)) FROM materialization_prefixes"
            ).fetchone()
            assert count == state.prefix_rows and key_bytes == state.prefix_parameter_bytes
        # Flush the disposable DB only for observational backing-byte accounting;
        # it is not a checkpoint, selector, or mandatory output commit.
        commits_during_operation = state.commit_calls
        state.commit()
        return PlannerObservation(
            "parent-id-trie-proposal" if trie else "current-spooled-owner",
            members,
            depth,
            sum(len(canonical_json_bytes(item.materialization_hint)) for item in advice),
            members * depth,
            int(count),
            int(key_bytes or 0),
            state.sql_calls,
            commits_during_operation,
            database.stat().st_size,
            elapsed,
            hashlib.sha256(
                canonical_json_bytes(
                    [
                        {
                            "artifact_id": row.artifact_id,
                            "components": list(row.components),
                            "sidecar_components": list(row.sidecar_components),
                            "reason": row.reason,
                            "materialization_hint": (
                                list(row.materialization_hint)
                                if row.materialization_hint is not None
                                else None
                            ),
                        }
                        for row in rows
                    ]
                )
            ).hexdigest(),
        )
    finally:
        state.close()


def observe_publication_barriers(directory: Path) -> dict[str, object]:
    """Execute actual SDK and CLI publication, recording native fsync targets."""
    directory.mkdir(parents=True, exist_ok=False)
    payload = b"exact local materialization witness\n"
    digest = hashlib.sha256(payload).hexdigest()
    syncs: list[str] = []
    original = os.fsync

    def observe(descriptor: int) -> None:
        syncs.append("directory" if stat.S_ISDIR(os.fstat(descriptor).st_mode) else "file")
        original(descriptor)

    source = directory / "sdk-output"
    with patch.object(os, "fsync", observe):
        verified_download(
            (payload,), output=source, expected_bytes=len(payload), expected_sha256=digest
        )
    sdk_syncs = list(syncs)
    syncs.clear()
    with patch.object(os, "fsync", observe):
        _publish_file(source, directory / "cli-output", byte_count=len(payload), sha256=digest)
    return {
        "sdk_sync_targets": sdk_syncs,
        "cli_sync_targets": syncs,
        "exact_bytes": len(payload),
        "sha256": digest,
        "qualification": "observed runtime calls; no power-loss or other-platform proof",
    }


def run_observations(workspace: Path) -> dict[str, object]:
    workspace.mkdir(parents=True, exist_ok=False)
    samples: list[dict[str, object]] = []
    for members, depth in [(16, value) for value in (4, 8, 16, 32)] + [
        (value, 8) for value in (32, 64, 128)
    ]:
        pair = [
            observe_planner(
                workspace / f"{'trie' if trie else 'current'}-{members}-{depth}",
                members=members,
                depth=depth,
                trie=trie,
            )
            for trie in (False, True)
        ]
        assert pair[0].result_sha256 == pair[1].result_sha256
        samples.extend(asdict(observation) for observation in pair)
    return {
        "format": "reference-999-materialization-observation/v1",
        "scope": "actual owner SQL/calls plus reference trie; not a maintained performance gate",
        "planner": samples,
        "publication": observe_publication_barriers(workspace / "publication"),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("workspace", type=Path)
    parser.add_argument("output", type=Path)
    arguments = parser.parse_args()
    arguments.output.write_text(json.dumps(run_observations(arguments.workspace), indent=2) + "\n")


if __name__ == "__main__":
    main()
