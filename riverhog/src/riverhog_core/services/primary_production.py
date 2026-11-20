"""Incremental exact producer-record validation over one selected Native context."""

from __future__ import annotations

import base64
import binascii
import json
import sqlite3
from collections.abc import Mapping
from contextlib import closing
from typing import Any

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)
from riverhog_protocol.collection_production_provenance import (
    COLLECTION_PRODUCTION_CONTRACT_ID,
    COLLECTION_RECORD_FRAGMENT_BYTES_MAX,
    PRODUCER_SCHEMA_ID,
    RECORD_FRAGMENT_SCHEMA_ID,
    RECORD_MANIFEST_SCHEMA_ID,
    collection_production_contract,
)
from riverhog_provenance import ProvenanceValidationError
from sha256_continuation import CheckpointSHA256

PROPERTIES = {
    COLLECTION_PRODUCTION_CONTRACT_ID + "/producer": PRODUCER_SCHEMA_ID,
    COLLECTION_PRODUCTION_CONTRACT_ID + "/record-manifest": RECORD_MANIFEST_SCHEMA_ID,
    COLLECTION_PRODUCTION_CONTRACT_ID + "/record-fragment": RECORD_FRAGMENT_SCHEMA_ID,
}


def install(db: sqlite3.Connection) -> None:
    """Install only application-owned disposable relations, once per original."""
    for statement in (
        "CREATE TABLE primary_production_work (key TEXT PRIMARY KEY, state TEXT NOT NULL)",
        "CREATE TABLE primary_production_seen (key TEXT, assertion TEXT, "
        "PRIMARY KEY(key,assertion)) WITHOUT ROWID",
        "CREATE TABLE primary_producers (key TEXT, digest TEXT, bytes TEXT, "
        "PRIMARY KEY(key,digest,bytes)) WITHOUT ROWID",
        "CREATE TABLE primary_manifests (key TEXT, kind TEXT, digest TEXT, "
        "bytes TEXT, parts TEXT, PRIMARY KEY(key,kind,digest)) WITHOUT ROWID",
        "CREATE TABLE primary_fragments (key TEXT, kind TEXT, digest TEXT, "
        "width INTEGER, offset TEXT, assertion TEXT, bytes TEXT, payload TEXT, "
        "PRIMARY KEY(key,kind,digest,width,offset,assertion)) WITHOUT ROWID",
    ):
        with closing(db.execute(statement)):
            pass


def _declaration() -> Declaration:
    def constant(value: int, unit: tuple[tuple[str, int], ...] = COUNT) -> Expression:
        return Expression("constant", value=value, unit=unit)

    return Declaration(
        id="riverhog.primary-production-atom.growth/v1",
        authority="riverhog",
        subjects=("riverhog_core.services.primary_production.PrimaryProductionWork.advance",),
        scope="one selected-context producer preparation atom",
        starts_at="receive at most one Native-selected extension or one continuation",
        ends_at="commit one exact ingestion, set-relation or fragment-hashing checkpoint",
        dimensions=(
            Dimension("atom_octets", OCTETS, "Largest selected extension or stored fragment atom."),
            Dimension("retained_rows", COUNT, "Application-owned indexed metadata rows."),
        ),
        bounds=(
            Bound("extension_visits", COUNT, "upper", constant(1), "At most one input extension."),
            Bound("fragment_visits", COUNT, "upper", constant(1), "At most one stored fragment."),
            Bound(
                "record_hash_octets",
                OCTETS,
                "upper",
                constant(COLLECTION_RECORD_FRAGMENT_BYTES_MAX, OCTETS),
                "Exactly one admitted fragment can enter the streaming SHA-256 state.",
            ),
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                constant(12),
                "One savepoint, indexed metadata work and one state update/release.",
            ),
            Bound(
                "source_payload_octets",
                OCTETS,
                "equal",
                constant(0, OCTETS),
                "No original journal source is opened or hashed.",
            ),
            Bound(
                "native_validation_advances",
                COUNT,
                "equal",
                constant(0),
                "The input is selected from independently accepted Native facts.",
            ),
        ),
        conditions=(
            "The caller pins one exact source, historical anchor and delivery context.",
            "Native schemas validate each extension atom and its profile data shape.",
            "The original application owns the admitted connection and these private relations.",
        ),
        invariants=(
            "An incomplete record set never produces a successful producer-check memo.",
            "Producer source-context sets, manifests, contiguous fragments and hashes are exact.",
            "Source growth is stored on disk; no list of producer preimages is accumulated.",
            "Repeated original intake is idempotent by exact Native assertion identity.",
        ),
        limitations=(
            "Selection, metadata key widths, index maintenance and physical SQLite work are "
            "separate observed dimensions; statement counts do not claim constant I/O.",
            "The private checkpoint is process-owned and rebuilds after a real process restart.",
            "This is producer-record preparation, not member or terminal closure acceptance.",
        ),
    )


class PrimaryProductionWork:
    """Keep one bounded continuation; source-sized record state stays in SQLite."""

    def __init__(self, db: sqlite3.Connection, key: str) -> None:
        self.db, self.key = db, key

    def _read(self, sql: str, values: tuple[Any, ...] = ()) -> Any:
        with closing(self.db.execute(sql, values)) as rows:
            return rows.fetchone()

    def _write(self, sql: str, values: tuple[Any, ...] = ()) -> None:
        with closing(self.db.execute(sql, values)):
            pass

    def _state(self) -> dict[str, Any]:
        row = self._read("SELECT state FROM primary_production_work WHERE key=?", (self.key,))
        return {"phase": "intake"} if row is None else json.loads(row[0])

    @property
    def accepting(self) -> bool:
        return bool(self._state()["phase"] == "intake")

    @property
    def complete(self) -> bool:
        return bool(self._state()["phase"] == "complete")

    @declare_growth(_declaration())
    def advance(self, row: Mapping[str, Any] | None = None) -> bool:
        """Consume one selected extension, or progress one finalization atom.

        A None input ends intake. After that, every call handles one set member,
        manifest transition or fragment. No member cursor may advance until True.
        """
        self._write("SAVEPOINT primary_production_atom")
        try:
            state = self._state()
            phase = state["phase"]
            if phase == "complete":
                if row is not None:
                    raise ValueError("completed producer preparation cannot accept another input")
            elif phase == "intake":
                if row is None:
                    if (
                        self._read(
                            "SELECT 1 FROM primary_producers WHERE key=? LIMIT 1", (self.key,)
                        )
                        is None
                    ):
                        raise ProvenanceValidationError(
                            "primary member journal lacks producer attribution"
                        )
                    state = {"phase": "sources"}
                else:
                    self._ingest(row)
            elif row is not None:
                raise ValueError("producer input arrived after its exact set was sealed")
            elif phase == "sources":
                state = self._source(state)
            elif phase == "orphans":
                state = self._orphan(state)
            elif phase == "manifests":
                state = self._manifest(state)
            elif phase == "fragments":
                state = self._fragment(state)
            else:
                raise RuntimeError("producer preparation lost its exact phase")
            self._write(
                "INSERT INTO primary_production_work VALUES (?,?) "
                "ON CONFLICT(key) DO UPDATE SET state=excluded.state",
                (self.key, json.dumps(state, separators=(",", ":"), sort_keys=True)),
            )
        except BaseException:
            self._write("ROLLBACK TO primary_production_atom")
            self._write("RELEASE primary_production_atom")
            raise
        self._write("RELEASE primary_production_atom")
        return bool(state["phase"] == "complete")

    def _ingest(self, row: Mapping[str, Any]) -> None:
        schema = PROPERTIES.get(row["property"])
        if schema is None:
            raise ValueError("producer selector returned an unrelated property")
        if self._read(
            "SELECT 1 FROM primary_production_seen WHERE key=? AND assertion=?",
            (self.key, row["assertion_id"]),
        ):
            return
        value = row["value"]
        if value["type"] != "json":
            raise ProvenanceValidationError("producer record is not a canonical profile value")
        profile = value["value"]
        contract = collection_production_contract()
        if profile["profile"] != {
            "contract_id": contract.contract_id,
            "contract_sha256": contract.contract_sha256,
            "schema_id": schema,
        }:
            raise ProvenanceValidationError("producer record profile does not match its property")
        data = profile["data"]
        if schema == PRODUCER_SCHEMA_ID:
            self._write(
                "INSERT OR IGNORE INTO primary_producers VALUES (?,?,?)",
                (self.key, data["source_context_sha256"], data["source_context_bytes"]),
            )
        elif schema == RECORD_MANIFEST_SCHEMA_ID:
            if self._read(
                "SELECT 1 FROM primary_manifests WHERE key=? AND kind=? AND digest=?",
                (self.key, data["record_kind"], data["record_sha256"]),
            ):
                raise ProvenanceValidationError("producer record manifest is duplicated")
            self._write(
                "INSERT INTO primary_manifests VALUES (?,?,?,?,?)",
                (
                    self.key,
                    data["record_kind"],
                    data["record_sha256"],
                    data["total_bytes"],
                    data["part_count"],
                ),
            )
        else:
            self._write(
                "INSERT INTO primary_fragments VALUES (?,?,?,?,?,?,?,?)",
                (
                    self.key,
                    data["record_kind"],
                    data["record_sha256"],
                    len(data["offset"]),
                    data["offset"],
                    row["assertion_id"],
                    data["total_bytes"],
                    data["data_base64"],
                ),
            )
        self._write(
            "INSERT INTO primary_production_seen VALUES (?,?)", (self.key, row["assertion_id"])
        )

    def _source(self, state: dict[str, Any]) -> dict[str, Any]:
        after = state.get("after", ("", ""))
        row = self._read(
            "SELECT digest,bytes FROM primary_producers WHERE key=? AND (digest,bytes)>(?,?) "
            "ORDER BY digest,bytes LIMIT 1",
            (self.key, *after),
        )
        if row is None:
            return {"phase": "orphans"}
        if (
            self._read(
                "SELECT 1 FROM primary_manifests WHERE key=? AND kind='source-context' "
                "AND digest=? AND bytes=?",
                (self.key, *row),
            )
            is None
        ):
            raise ProvenanceValidationError("producer source context has no exact record manifest")
        return {"phase": "sources", "after": row}

    def _orphan(self, state: dict[str, Any]) -> dict[str, Any]:
        after = state.get("after", ("", ""))
        row = self._read(
            "SELECT kind,digest FROM primary_fragments WHERE key=? AND (kind,digest)>(?,?) "
            "ORDER BY kind,digest,width,offset,assertion LIMIT 1",
            (self.key, *after),
        )
        if row is None:
            return {"phase": "manifests"}
        if (
            self._read(
                "SELECT 1 FROM primary_manifests WHERE key=? AND kind=? AND digest=?",
                (self.key, *row),
            )
            is None
        ):
            raise ProvenanceValidationError("producer fragments and manifests differ")
        return {"phase": "orphans", "after": row}

    def _manifest(self, state: dict[str, Any]) -> dict[str, Any]:
        after = state.get("after", ("", ""))
        row = self._read(
            "SELECT kind,digest,bytes,parts FROM primary_manifests "
            "WHERE key=? AND (kind,digest)>(?,?) ORDER BY kind,digest LIMIT 1",
            (self.key, *after),
        )
        if row is None:
            return {"phase": "complete"}
        kind, digest, size, parts = row
        if (
            kind == "source-context"
            and self._read(
                "SELECT 1 FROM primary_producers WHERE key=? AND digest=? AND bytes=?",
                (self.key, digest, size),
            )
            is None
        ):
            raise ProvenanceValidationError("producer source context has no exact record manifest")
        return {
            "phase": "fragments",
            "kind": kind,
            "digest": digest,
            "bytes": size,
            "parts": parts,
            "count": 0,
            "offset": 0,
            "hash": CheckpointSHA256().export_state(),
        }

    def _fragment(self, state: dict[str, Any]) -> dict[str, Any]:
        after = state.get("after", (0, "", ""))
        row = self._read(
            "SELECT width,offset,assertion,bytes,payload FROM primary_fragments "
            "WHERE key=? AND kind=? AND digest=? AND (width,offset,assertion)>(?,?,?) "
            "ORDER BY width,offset,assertion LIMIT 1",
            (self.key, state["kind"], state["digest"], *after),
        )
        digest = CheckpointSHA256.from_state(state["hash"])
        if row is None:
            if state["count"] != int(state["parts"]):
                raise ProvenanceValidationError("producer record part count differs")
            if state["offset"] != int(state["bytes"]) or digest.hexdigest() != state["digest"]:
                raise ProvenanceValidationError(
                    "producer record preimage differs from its manifest"
                )
            return {"phase": "manifests", "after": (state["kind"], state["digest"])}
        width, offset, identity, total, encoded = row
        if int(offset) != state["offset"] or total != state["bytes"]:
            raise ProvenanceValidationError("producer record fragments are not contiguous")
        try:
            data = base64.b64decode(encoded, validate=True)
        except (binascii.Error, ValueError) as error:
            raise ProvenanceValidationError("producer fragment is not base64") from error
        if (
            not data
            or len(data) > COLLECTION_RECORD_FRAGMENT_BYTES_MAX
            or base64.b64encode(data).decode("ascii") != encoded
        ):
            raise ProvenanceValidationError("producer fragment exceeds its exact byte contract")
        digest.update(data)
        return {
            **state,
            "offset": state["offset"] + len(data),
            "count": state["count"] + 1,
            "after": (width, offset, identity),
            "hash": digest.export_state(),
        }
