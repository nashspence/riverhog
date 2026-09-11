"""Restartable finalized-receipt custody transfer for the FTP intake directory."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import shutil
import sqlite3
import stat
import threading
from bisect import bisect_right
from collections.abc import Mapping, Sequence
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from riverhog_client import ApiClient
from riverhog_client.producer import CollectionProducer, ProducedCollection, ProducerFile
from riverhog_provenance import (
    SIDECAR_SUFFIX,
    FileProvenanceBinding,
    FileStateObserverFactory,
    canonical_sidecar_path,
    prepare_file_provenance,
)

from riverhog_ftp_adapter.completion import (
    CONTROL_DIR,
    MAX_COMPLETION_RECORD_BYTES,
    CompletionError,
    CompletionRecord,
    completion_log_path,
    initialize_completion_authority,
    parse_completion_record,
)
from riverhog_ftp_adapter.config import FtpAdapterConfig, SourceConfig

_CONTROL_DIR = CONTROL_DIR
_FLUSH_MARKER = ".riverhog-ftp-flush"
_MANIFEST = "claim.json"
_RECEIPT = "receipt.json"
_RECEIPTS_DIR = "receipts"
_STATE_DB = "state.sqlite3"


@dataclass(frozen=True, slots=True)
class _Admission:
    claim_root: Path | None
    claim_ordinal: int | None
    entries_examined: int
    sweep_complete: bool


@dataclass(frozen=True, slots=True)
class _ClaimWork:
    ordinal: int
    root: Path


@dataclass(frozen=True, slots=True)
class _DiscoveredFile:
    path: Path
    relative: str
    observed: os.stat_result
    event_identity: str


@dataclass(frozen=True, slots=True)
class _DiscoveryBatch:
    files: tuple[_DiscoveredFile, ...]
    entries_examined: int
    sweep_complete: bool
    generation: str
    next_offset: int


class FtpAdapterError(RuntimeError):
    pass


class SourceChanged(FtpAdapterError):
    pass


class ClaimCollision(FtpAdapterError):
    pass


class FtpAdapter:
    """Move completed inputs into bounded scratch until Riverhog finalizes them.

    The claim intent is fsynced before the first rename. Reconciliation can
    therefore finish every partially claimed batch after process or host loss.
    Payload bytes are removed only after an exact finalized Riverhog receipt.
    """

    def __init__(
        self,
        api: ApiClient,
        config: FtpAdapterConfig,
        *,
        provenance_observer_factory: FileStateObserverFactory | None = None,
    ) -> None:
        self.api = api
        self.config = config
        self._provenance_observer_factory = provenance_observer_factory
        self._custody_pass_lock = threading.Lock()
        for source in config.sources:
            self._initialize_source(source)

    def _control_root(self, source: SourceConfig) -> Path:
        return source.root / _CONTROL_DIR

    def _state_path(self, source: SourceConfig) -> Path:
        return self._control_root(source) / _STATE_DB

    def _completion_log_path(self, source: SourceConfig) -> Path:
        return completion_log_path(source.root)

    def _open_state(self, source: SourceConfig) -> sqlite3.Connection:
        connection = sqlite3.connect(self._state_path(source))
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def _initialize_source(self, source: SourceConfig) -> None:
        source.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._control_root(source).mkdir(mode=0o700, parents=True, exist_ok=True)
        with closing(self._open_state(source)) as connection:
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if version not in {0, 2}:
                raise FtpAdapterError("unsupported FTP adapter operational-state revision")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS claims (
                    ordinal INTEGER PRIMARY KEY,
                    claim_id TEXT NOT NULL UNIQUE,
                    manifest_json TEXT NOT NULL,
                    claim_bytes INTEGER NOT NULL CHECK (claim_bytes >= 0)
                );
                CREATE TABLE IF NOT EXISTS adapter_state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS completion_events (
                    event_id TEXT PRIMARY KEY,
                    claim_id TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS completion_failures (
                    ordinal INTEGER PRIMARY KEY,
                    failure_id TEXT NOT NULL UNIQUE,
                    generation TEXT NOT NULL,
                    record_offset INTEGER NOT NULL CHECK (record_offset >= 0),
                    raw BLOB NOT NULL,
                    reason TEXT NOT NULL,
                    retryable INTEGER NOT NULL CHECK (retryable IN (0, 1)),
                    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0)
                );
                PRAGMA user_version = 2;
                """
            )
            claim_count = _state_value(connection, "claim_count")
            claim_bytes = _state_value(connection, "claim_bytes")
            if claim_count is None or claim_bytes is None:
                existing_claims = int(
                    connection.execute("SELECT COUNT(*) FROM claims").fetchone()[0]
                )
                if existing_claims:
                    raise FtpAdapterError(
                        "FTP claim accounting is absent for existing operational custody"
                    )
                _set_state_value(connection, "claim_count", "0")
                _set_state_value(connection, "claim_bytes", "0")
            failure_count = _state_value(connection, "completion_failure_count")
            if failure_count is None:
                existing_failures = int(
                    connection.execute("SELECT COUNT(*) FROM completion_failures").fetchone()[0]
                )
                if existing_failures:
                    raise FtpAdapterError(
                        "FTP completion failure accounting is absent for existing obligations"
                    )
                _set_state_value(connection, "completion_failure_count", "0")
            connection.commit()
        generation, header_bytes = initialize_completion_authority(source.root)
        with closing(self._open_state(source)) as connection:
            stored_generation = _state_value(connection, "completion_generation")
            stored_offset = _state_value(connection, "completion_offset")
            if stored_generation is None:
                connection.execute(
                    "INSERT INTO adapter_state(key, value) VALUES (?, ?)",
                    ("completion_generation", generation),
                )
                connection.execute(
                    "INSERT INTO adapter_state(key, value) VALUES (?, ?)",
                    ("completion_offset", str(header_bytes)),
                )
                connection.commit()
            elif stored_generation != generation:
                raise FtpAdapterError("FTP completion log changed outside adapter custody")
            elif stored_offset is None or int(stored_offset) < header_bytes:
                raise FtpAdapterError("FTP completion cursor is invalid")

    def run_once(self, source_ids: Sequence[str] | None = None) -> dict[str, object]:
        with self._custody_pass_lock:
            return self._run_once(source_ids)

    def _run_once(self, source_ids: Sequence[str] | None = None) -> dict[str, object]:
        selected = set(source_ids or ())
        sources = tuple(
            source for source in self.config.sources if not selected or source.id in selected
        )
        unknown = selected - {source.id for source in sources}
        if unknown:
            raise KeyError(", ".join(sorted(unknown)))
        completed = 0
        failed: list[dict[str, str]] = []
        source_results: list[dict[str, object]] = []
        for source in sources:
            failure_attempts = self._reconcile_completion_failures(source)
            claim_count, _claim_bytes = self._claim_summary(source)
            capacity_available = claim_count < self.config.pending_claim_capacity
            retry_budget = self.config.claim_attempt_budget - int(capacity_available)
            work = self._claim_work(source, limit=retry_budget)
            source_completed = 0
            source_failed = 0
            attempts = 0
            for claim in work:
                try:
                    self._publish_claim(source, claim.root)
                except Exception as exc:
                    failed.append(
                        {
                            "source": source.id,
                            "claim": claim.root.name,
                            "error": str(exc)[:1000],
                        }
                    )
                    source_failed += 1
                else:
                    completed += 1
                    source_completed += 1
                finally:
                    attempts += 1
                    self._save_reconcile_cursor(source, claim.ordinal)
            pending_after_work = claim_count - source_completed
            admission = _Admission(None, None, 0, False)
            if (
                attempts < self.config.claim_attempt_budget
                and pending_after_work < self.config.pending_claim_capacity
            ):
                try:
                    admission = self._claim_new_batch(source)
                    if admission.claim_root is not None:
                        attempts += 1
                        try:
                            self._publish_claim(source, admission.claim_root)
                        except Exception as exc:
                            failed.append(
                                {
                                    "source": source.id,
                                    "claim": admission.claim_root.name,
                                    "error": str(exc)[:1000],
                                }
                            )
                            source_failed += 1
                        else:
                            completed += 1
                            source_completed += 1
                        finally:
                            if admission.claim_ordinal is None:
                                raise FtpAdapterError("new FTP claim has no durable ordinal")
                            self._save_reconcile_cursor(source, admission.claim_ordinal)
                except Exception as exc:
                    failed.append({"source": source.id, "claim": "new", "error": str(exc)[:1000]})
                    source_failed += 1
            source_results.append(
                {
                    "id": source.id,
                    "claim_attempts": attempts,
                    "completed": source_completed,
                    "failed": source_failed,
                    "discovery_entries_examined": admission.entries_examined,
                    "discovery_sweep_complete": admission.sweep_complete,
                    "admission_deferred": (
                        pending_after_work >= self.config.pending_claim_capacity
                    ),
                    "completion_failure_attempts": failure_attempts,
                    "completion_failures": self._completion_failure_count(source),
                }
            )
        return {
            "format": "riverhog-ftp-adapter-pass/v1",
            "completed": completed,
            "failed": failed,
            "sources": [source.id for source in sources],
            "source_results": source_results,
        }

    def flush(self, source_id: str) -> dict[str, object]:
        source = self.config.source(source_id)
        with self._custody_pass_lock:
            marker = source.root / _FLUSH_MARKER
            marker.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            _write_atomic(marker, b"riverhog-ftp-adapter-flush/v1\n")
            return self._run_once((source_id,))

    def status(
        self,
        *,
        page_size: int = 25,
        page_token: str | None = None,
    ) -> dict[str, object]:
        with self._custody_pass_lock:
            return self._status(page_size=page_size, page_token=page_token)

    def _status(
        self,
        *,
        page_size: int,
        page_token: str | None,
    ) -> dict[str, object]:
        if page_size < 1 or page_size > 100:
            raise ValueError("FTP adapter status page_size must be in 1..100")
        source_ids = [source.id for source in self.config.sources]
        start = bisect_right(source_ids, page_token) if page_token is not None else 0
        selected = self.config.sources[start : start + page_size]
        rows: list[dict[str, object]] = []
        for source in selected:
            claim_count, claim_bytes = self._claim_summary(source)
            failure_count = self._completion_failure_count(source)
            rows.append(
                {
                    "id": source.id,
                    "ingest_source": source.ingest_source,
                    "claims": claim_count,
                    "claim_bytes": claim_bytes,
                    "close_mode": source.close_mode,
                    "max_files": source.max_files,
                    "max_bytes": source.max_bytes,
                    "provenance": source.provenance,
                    "pending_claim_capacity": self.config.pending_claim_capacity,
                    "completion_failures": failure_count,
                    "completion_failure_capacity": self.config.completion_failure_capacity,
                    "oldest_completion_failure": self._oldest_completion_failure(source),
                }
            )
        next_page_token = (
            selected[-1].id if start + len(selected) < len(self.config.sources) else None
        )
        return {
            "format": "riverhog-ftp-adapter-status/v1",
            "provenance_observer": self.config.provenance_observer,
            "sources": rows,
            "page_size": page_size,
            "next_page_token": next_page_token,
            "snapshot": False,
        }

    def accept_completed_file(
        self,
        source: SourceConfig,
        path: Path,
        *,
        relative_path: str,
        source_event_id: str,
        expected_bytes: int,
        expected_sha256: str,
        provenance: Mapping[str, object] | None = None,
        provenance_journals: Mapping[str, bytes] | None = None,
    ) -> ProducedCollection:
        """Claim one protocol-complete file and reconcile it to a receipt."""

        with self._custody_pass_lock:
            return self._accept_completed_file(
                source,
                path,
                relative_path=relative_path,
                source_event_id=source_event_id,
                expected_bytes=expected_bytes,
                expected_sha256=expected_sha256,
                provenance=provenance,
                provenance_journals=provenance_journals,
            )

    def _accept_completed_file(
        self,
        source: SourceConfig,
        path: Path,
        *,
        relative_path: str,
        source_event_id: str,
        expected_bytes: int,
        expected_sha256: str,
        provenance: Mapping[str, object] | None = None,
        provenance_journals: Mapping[str, bytes] | None = None,
    ) -> ProducedCollection:

        identity = _completed_claim_identity(
            source.id,
            source_event_id,
            relative_path,
            expected_bytes,
            expected_sha256,
        )
        claim_root = self._claims_root(source) / identity
        durable_receipt = self._durable_receipt(source, identity)
        if durable_receipt is not None:
            if claim_root.exists():
                shutil.rmtree(claim_root)
            self._forget_claim(source, identity)
            return durable_receipt
        if self._claim_record(source, identity) is not None:
            self._materialize_registered_claim(source, identity)
            self._reconcile_claim(source, claim_root)
            return self._publish_claim(source, claim_root)
        if not path.exists():
            raise SourceChanged("completed protocol upload is missing and has no receipt")
        claim_count, _claim_bytes = self._claim_summary(source)
        if claim_count >= self.config.pending_claim_capacity:
            raise FtpAdapterError("pending claim capacity is exhausted; admission is deferred")
        observed = path.stat(follow_symlinks=False)
        if not stat.S_ISREG(observed.st_mode) or observed.st_size != expected_bytes:
            raise SourceChanged("completed protocol upload differs from its declared size")
        if _sha256_path(path) != expected_sha256:
            raise SourceChanged("completed protocol upload differs from its declared SHA-256")
        if not claim_root.exists():
            claim_root.mkdir(mode=0o700, parents=True)
            try:
                journals = dict(provenance_journals or {})
                if provenance is None:
                    binding, captured = self._prepared_provenance(source, path, relative_path)
                    journals.update(captured)
                else:
                    binding = dict(provenance)
                manifest = {
                    "format": "riverhog-ftp-adapter-claim/v1",
                    "claim_id": identity,
                    "source_event_id": source_event_id,
                    "source": source.id,
                    "files": [
                        {
                            "path": relative_path,
                            "bytes": observed.st_size,
                            "sha256": expected_sha256,
                            "device": observed.st_dev,
                            "inode": observed.st_ino,
                            "original": str(path.resolve()),
                            "provenance": binding,
                        }
                    ],
                    "journals": self._persist_journals(claim_root, journals),
                }
                _write_json(claim_root / _MANIFEST, manifest)
                self._register_claim(source, manifest)
            except BaseException:
                shutil.rmtree(claim_root)
                raise
        self._reconcile_claim(source, claim_root)
        return self._publish_claim(source, claim_root)

    def _claims_root(self, source: SourceConfig) -> Path:
        return source.root / _CONTROL_DIR / "claims"

    def _claim_summary(self, source: SourceConfig) -> tuple[int, int]:
        with closing(self._open_state(source)) as connection:
            claim_count = _state_value(connection, "claim_count")
            claim_bytes = _state_value(connection, "claim_bytes")
        if claim_count is None or claim_bytes is None:
            raise FtpAdapterError("FTP claim accounting is unavailable")
        return int(claim_count), int(claim_bytes)

    def _claim_work(
        self,
        source: SourceConfig,
        *,
        limit: int,
    ) -> tuple[_ClaimWork, ...]:
        if limit < 1:
            return ()
        with closing(self._open_state(source)) as connection:
            raw_after = _state_value(connection, "reconcile_after")
            after = int(raw_after) if raw_after is not None else 0
            rows = connection.execute(
                "SELECT ordinal, claim_id FROM claims WHERE ordinal > ? ORDER BY ordinal LIMIT ?",
                (after, limit),
            ).fetchall()
            if len(rows) < limit:
                rows.extend(
                    connection.execute(
                        "SELECT ordinal, claim_id FROM claims "
                        "WHERE ordinal <= ? ORDER BY ordinal LIMIT ?",
                        (after, limit - len(rows)),
                    ).fetchall()
                )
        return tuple(
            _ClaimWork(int(ordinal), self._claims_root(source) / str(claim_id))
            for ordinal, claim_id in rows
        )

    def _save_reconcile_cursor(self, source: SourceConfig, ordinal: int) -> None:
        with closing(self._open_state(source)) as connection:
            _set_state_value(connection, "reconcile_after", str(ordinal))
            connection.commit()

    def _completion_cursor(self, source: SourceConfig) -> tuple[str, int]:
        with closing(self._open_state(source)) as connection:
            generation = _state_value(connection, "completion_generation")
            raw_offset = _state_value(connection, "completion_offset")
        if generation is None or raw_offset is None:
            raise FtpAdapterError("FTP completion cursor is unavailable")
        return generation, int(raw_offset)

    def _save_completion_cursor(
        self,
        source: SourceConfig,
        *,
        generation: str,
        offset: int,
    ) -> None:
        with closing(self._open_state(source)) as connection:
            current_generation = _state_value(connection, "completion_generation")
            current_offset = _state_value(connection, "completion_offset")
            if current_generation != generation:
                raise FtpAdapterError("FTP completion authority changed during traversal")
            if current_offset is None or offset < int(current_offset):
                raise FtpAdapterError("FTP completion cursor moved backwards")
            _set_state_value(connection, "completion_offset", str(offset))
            connection.commit()

    def _completion_failure_count(self, source: SourceConfig) -> int:
        with closing(self._open_state(source)) as connection:
            value = _state_value(connection, "completion_failure_count")
        if value is None:
            raise FtpAdapterError("FTP completion failure accounting is unavailable")
        return int(value)

    def _oldest_completion_failure(self, source: SourceConfig) -> dict[str, object] | None:
        with closing(self._open_state(source)) as connection:
            row = connection.execute(
                "SELECT reason, retryable, attempts FROM completion_failures "
                "ORDER BY ordinal LIMIT 1"
            ).fetchone()
        if row is None:
            return None
        return {
            "reason": str(row[0]),
            "retryable": bool(row[1]),
            "attempts": int(row[2]),
        }

    def _known_completion_event(self, source: SourceConfig, event_id: str) -> bool:
        with closing(self._open_state(source)) as connection:
            row = connection.execute(
                "SELECT 1 FROM completion_events WHERE event_id = ?",
                (event_id,),
            ).fetchone()
        return row is not None

    def _record_completion_failure(
        self,
        source: SourceConfig,
        *,
        generation: str,
        record_offset: int,
        next_offset: int,
        raw: bytes,
        reason: str,
        retryable: bool,
    ) -> None:
        failure_id = hashlib.sha256(
            b"\0".join(
                (
                    b"riverhog-ftp-completion-failure/v1",
                    generation.encode("ascii"),
                    str(record_offset).encode("ascii"),
                    raw,
                )
            )
        ).hexdigest()
        with closing(self._open_state(source)) as connection:
            current_generation = _state_value(connection, "completion_generation")
            current_offset = _state_value(connection, "completion_offset")
            current_count = _state_value(connection, "completion_failure_count")
            if (
                current_generation != generation
                or current_offset is None
                or int(current_offset) > record_offset
                or current_count is None
            ):
                raise FtpAdapterError("FTP completion failure cursor changed during admission")
            existing = connection.execute(
                "SELECT raw, reason, retryable FROM completion_failures WHERE failure_id = ?",
                (failure_id,),
            ).fetchone()
            if existing is None:
                if int(current_count) >= self.config.completion_failure_capacity:
                    raise FtpAdapterError(
                        "completion failure capacity is exhausted; FTP admission is deferred"
                    )
                connection.execute(
                    "INSERT INTO completion_failures("
                    "failure_id, generation, record_offset, raw, reason, retryable"
                    ") VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        failure_id,
                        generation,
                        record_offset,
                        raw,
                        reason[:1000],
                        int(retryable),
                    ),
                )
                _set_state_value(
                    connection,
                    "completion_failure_count",
                    str(int(current_count) + 1),
                )
            elif (bytes(existing[0]), str(existing[1]), bool(existing[2])) != (
                raw,
                reason[:1000],
                retryable,
            ):
                raise ClaimCollision("FTP completion failure identity collision")
            _set_state_value(connection, "completion_offset", str(next_offset))
            connection.commit()

    def _reconcile_completion_failures(self, source: SourceConfig) -> int:
        budget = self.config.completion_failure_attempt_budget
        with closing(self._open_state(source)) as connection:
            raw_after = _state_value(connection, "completion_failure_after")
            after = int(raw_after) if raw_after is not None else 0
            rows = connection.execute(
                "SELECT ordinal, failure_id, raw FROM completion_failures "
                "WHERE retryable = 1 AND ordinal > ? ORDER BY ordinal LIMIT ?",
                (after, budget),
            ).fetchall()
            if len(rows) < budget:
                rows.extend(
                    connection.execute(
                        "SELECT ordinal, failure_id, raw FROM completion_failures "
                        "WHERE retryable = 1 AND ordinal <= ? ORDER BY ordinal LIMIT ?",
                        (after, budget - len(rows)),
                    ).fetchall()
                )
        attempts = 0
        for ordinal, failure_id, raw_value in rows:
            attempts += 1
            raw = bytes(raw_value)
            recovered = False
            try:
                record = parse_completion_record(raw)
                if record.source_id != source.id:
                    raise CompletionError("FTP completion record names another source")
                path = source.root / record.custody
                observed = path.stat(follow_symlinks=False)
                _require_completion_identity(observed, record)
            except (CompletionError, OSError, SourceChanged):
                pass
            else:
                self._append_completion_record(source, raw)
                recovered = True
            with closing(self._open_state(source)) as connection:
                current_count = _state_value(connection, "completion_failure_count")
                if current_count is None:
                    raise FtpAdapterError("FTP completion failure accounting is unavailable")
                if recovered:
                    deleted = connection.execute(
                        "DELETE FROM completion_failures WHERE failure_id = ?",
                        (str(failure_id),),
                    ).rowcount
                    if deleted:
                        _set_state_value(
                            connection,
                            "completion_failure_count",
                            str(int(current_count) - 1),
                        )
                else:
                    connection.execute(
                        "UPDATE completion_failures SET attempts = attempts + 1 "
                        "WHERE failure_id = ?",
                        (str(failure_id),),
                    )
                _set_state_value(connection, "completion_failure_after", str(int(ordinal)))
                connection.commit()
        return attempts

    def _append_completion_record(self, source: SourceConfig, raw: bytes) -> None:
        path = self._completion_log_path(source)
        with path.open("ab") as stream:
            fcntl.lockf(stream.fileno(), fcntl.LOCK_EX)
            try:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            finally:
                fcntl.lockf(stream.fileno(), fcntl.LOCK_UN)

    def _claim_record(self, source: SourceConfig, claim_id: str) -> tuple[int, str] | None:
        with closing(self._open_state(source)) as connection:
            row = connection.execute(
                "SELECT ordinal, manifest_json FROM claims WHERE claim_id = ?",
                (claim_id,),
            ).fetchone()
        return None if row is None else (int(row[0]), str(row[1]))

    def _register_claim(
        self,
        source: SourceConfig,
        manifest: Mapping[str, object],
        *,
        discovery_generation: str | None = None,
        discovery_offset: int | None = None,
    ) -> int:
        claim_id = str(manifest["claim_id"])
        encoded = json.dumps(manifest, sort_keys=True, separators=(",", ":"))
        claim_bytes = sum(int(str(row["bytes"])) for row in _file_rows(manifest))
        with closing(self._open_state(source)) as connection:
            existing = connection.execute(
                "SELECT ordinal, manifest_json FROM claims WHERE claim_id = ?",
                (claim_id,),
            ).fetchone()
            if existing is None:
                cursor = connection.execute(
                    "INSERT INTO claims(claim_id, manifest_json, claim_bytes) VALUES (?, ?, ?)",
                    (claim_id, encoded, claim_bytes),
                )
                if cursor.lastrowid is None:
                    raise FtpAdapterError("FTP claim registration returned no durable ordinal")
                ordinal = int(cursor.lastrowid)
                current_count = _state_value(connection, "claim_count")
                current_bytes = _state_value(connection, "claim_bytes")
                if current_count is None or current_bytes is None:
                    raise FtpAdapterError("FTP claim accounting is unavailable")
                _set_state_value(connection, "claim_count", str(int(current_count) + 1))
                _set_state_value(
                    connection,
                    "claim_bytes",
                    str(int(current_bytes) + claim_bytes),
                )
            else:
                ordinal = int(existing[0])
                if str(existing[1]) != encoded:
                    raise ClaimCollision(f"claim identity collision: {claim_id}")
            raw_event_ids = manifest.get("completion_event_ids", [])
            if not isinstance(raw_event_ids, list) or any(
                not isinstance(item, str) for item in raw_event_ids
            ):
                raise FtpAdapterError("FTP claim completion event identities are invalid")
            for event_id in raw_event_ids:
                claimed = connection.execute(
                    "SELECT claim_id FROM completion_events WHERE event_id = ?",
                    (event_id,),
                ).fetchone()
                if claimed is None:
                    connection.execute(
                        "INSERT INTO completion_events(event_id, claim_id) VALUES (?, ?)",
                        (event_id, claim_id),
                    )
                elif str(claimed[0]) != claim_id:
                    raise ClaimCollision(f"completion event was claimed twice: {event_id}")
            if discovery_generation is not None or discovery_offset is not None:
                if discovery_generation is None or discovery_offset is None:
                    raise FtpAdapterError("incomplete FTP completion cursor update")
                current_generation = _state_value(connection, "completion_generation")
                if current_generation != discovery_generation:
                    raise FtpAdapterError("FTP completion authority changed during admission")
                _set_state_value(connection, "completion_offset", str(discovery_offset))
            connection.commit()
        return ordinal

    def _materialize_registered_claim(self, source: SourceConfig, claim_id: str) -> Path:
        record = self._claim_record(source, claim_id)
        if record is None:
            raise FtpAdapterError(f"FTP claim is not registered: {claim_id}")
        _ordinal, encoded = record
        claim_root = self._claims_root(source) / claim_id
        manifest_path = claim_root / _MANIFEST
        if not manifest_path.is_file():
            payload = json.loads(encoded)
            if not isinstance(payload, dict):
                raise FtpAdapterError("registered FTP claim manifest is invalid")
            _write_json(manifest_path, payload)
        return claim_root

    def _forget_claim(self, source: SourceConfig, claim_id: str) -> None:
        with closing(self._open_state(source)) as connection:
            row = connection.execute(
                "SELECT claim_bytes FROM claims WHERE claim_id = ?",
                (claim_id,),
            ).fetchone()
            if row is not None:
                current_count = _state_value(connection, "claim_count")
                current_bytes = _state_value(connection, "claim_bytes")
                if current_count is None or current_bytes is None:
                    raise FtpAdapterError("FTP claim accounting is unavailable")
                next_count = int(current_count) - 1
                next_bytes = int(current_bytes) - int(row[0])
                if next_count < 0 or next_bytes < 0:
                    raise FtpAdapterError("FTP claim accounting is inconsistent")
                connection.execute("DELETE FROM claims WHERE claim_id = ?", (claim_id,))
                _set_state_value(connection, "claim_count", str(next_count))
                _set_state_value(connection, "claim_bytes", str(next_bytes))
            connection.commit()

    def _discover_batch(self, source: SourceConfig, *, flush: bool) -> _DiscoveryBatch:
        if source.close_mode == "explicit-flush" and not flush:
            generation, offset = self._completion_cursor(source)
            return _DiscoveryBatch((), 0, True, generation, offset)
        generation, offset = self._completion_cursor(source)
        selected: list[_DiscoveredFile] = []
        total = 0
        examined = 0
        next_offset = offset
        complete = False
        log_path = self._completion_log_path(source)
        selected_events: set[str] = set()
        with log_path.open("rb") as stream:
            fcntl.lockf(stream.fileno(), fcntl.LOCK_SH)
            try:
                if os.fstat(stream.fileno()).st_size < offset:
                    raise FtpAdapterError("FTP completion log ended before its durable cursor")
                stream.seek(offset)
                while examined < self.config.discovery_entry_budget:
                    if selected and (
                        len(selected) >= source.max_files or total >= source.max_bytes
                    ):
                        break
                    record_start = stream.tell()
                    raw = stream.readline(MAX_COMPLETION_RECORD_BYTES + 1)
                    if not raw:
                        complete = True
                        break
                    if len(raw) > MAX_COMPLETION_RECORD_BYTES:
                        if selected:
                            break
                        self._record_completion_failure(
                            source,
                            generation=generation,
                            record_offset=record_start,
                            next_offset=stream.tell(),
                            raw=raw,
                            reason="FTP completion record exceeds its protocol bound",
                            retryable=False,
                        )
                        next_offset = stream.tell()
                        examined += 1
                        continue
                    if not raw.endswith(b"\n"):
                        break
                    record_end = stream.tell()
                    examined += 1
                    try:
                        record = parse_completion_record(raw)
                        if record.source_id != source.id:
                            raise CompletionError("FTP completion record names another source")
                    except CompletionError as exc:
                        if selected:
                            next_offset = record_start
                            break
                        self._record_completion_failure(
                            source,
                            generation=generation,
                            record_offset=record_start,
                            next_offset=record_end,
                            raw=raw,
                            reason=str(exc),
                            retryable=False,
                        )
                        next_offset = record_end
                        continue
                    if record.event_id in selected_events or self._known_completion_event(
                        source, record.event_id
                    ):
                        next_offset = record_end
                        continue
                    if record.path.endswith(SIDECAR_SUFFIX):
                        next_offset = record_end
                        continue
                    if selected and total + record.bytes > source.max_bytes:
                        next_offset = record_start
                        break
                    path = source.root / record.custody
                    try:
                        observed = path.stat(follow_symlinks=False)
                        _require_completion_identity(observed, record)
                    except (FileNotFoundError, SourceChanged) as exc:
                        if selected:
                            next_offset = record_start
                            break
                        self._record_completion_failure(
                            source,
                            generation=generation,
                            record_offset=record_start,
                            next_offset=record_end,
                            raw=raw,
                            reason=str(exc),
                            retryable=True,
                        )
                        next_offset = record_end
                        continue
                    selected.append(_DiscoveredFile(path, record.path, observed, record.event_id))
                    selected_events.add(record.event_id)
                    total += observed.st_size
                    next_offset = record_end
            finally:
                fcntl.lockf(stream.fileno(), fcntl.LOCK_UN)
        selected.sort(key=lambda row: row.relative.encode("utf-8"))
        return _DiscoveryBatch(
            tuple(selected),
            examined,
            complete,
            generation,
            next_offset,
        )

    def _claim_new_batch(self, source: SourceConfig) -> _Admission:
        marker = source.root / _FLUSH_MARKER
        flush = marker.is_file()
        discovery = self._discover_batch(source, flush=flush)
        if not discovery.files:
            self._save_completion_cursor(
                source,
                generation=discovery.generation,
                offset=discovery.next_offset,
            )
            if flush and discovery.sweep_complete:
                marker.unlink(missing_ok=True)
            return _Admission(
                None,
                None,
                discovery.entries_examined,
                discovery.sweep_complete,
            )
        event_id = hashlib.sha256(
            "\n".join(
                f"{row.event_identity}\0{row.relative}\0{row.observed.st_size}"
                for row in discovery.files
            ).encode()
        ).hexdigest()
        claim_id = _claim_identity(
            source.id,
            event_id,
            tuple(
                (
                    row.relative,
                    row.observed.st_size,
                    row.observed.st_dev,
                    row.observed.st_ino,
                )
                for row in discovery.files
            ),
        )
        claim_root = self._claims_root(source) / claim_id
        registered = self._claim_record(source, claim_id)
        if registered is not None:
            self._save_completion_cursor(
                source,
                generation=discovery.generation,
                offset=discovery.next_offset,
            )
            self._materialize_registered_claim(source, claim_id)
            return _Admission(
                claim_root,
                registered[0],
                discovery.entries_examined,
                discovery.sweep_complete,
            )
        if claim_root.exists():
            if (claim_root / _MANIFEST).is_file():
                manifest = _read_manifest(claim_root)
                ordinal = self._register_claim(
                    source,
                    manifest,
                    discovery_generation=discovery.generation,
                    discovery_offset=discovery.next_offset,
                )
                return _Admission(
                    claim_root,
                    ordinal,
                    discovery.entries_examined,
                    discovery.sweep_complete,
                )
            # Finalized claim intent is the manifest. Construction can leave a
            # pre-manifest directory behind, but no payload move has begun yet.
            shutil.rmtree(claim_root)
        claim_root.mkdir(mode=0o700, parents=True)
        try:
            files: list[dict[str, object]] = []
            journals: dict[str, bytes] = {}
            for discovered in discovery.files:
                binding, captured = self._prepared_provenance(
                    source,
                    discovered.path,
                    discovered.relative,
                )
                journals.update(_merge_journals(journals, captured))
                files.append(
                    {
                        "path": discovered.relative,
                        "bytes": discovered.observed.st_size,
                        "sha256": _sha256_path(discovered.path),
                        "device": discovered.observed.st_dev,
                        "inode": discovered.observed.st_ino,
                        "original": str(discovered.path.resolve()),
                        "provenance": binding,
                    }
                )
            manifest = {
                "format": "riverhog-ftp-adapter-claim/v1",
                "claim_id": claim_id,
                "source_event_id": event_id,
                "source": source.id,
                "completion_event_ids": [row.event_identity for row in discovery.files],
                "files": files,
                "journals": self._persist_journals(claim_root, journals),
            }
            _write_json(claim_root / _MANIFEST, manifest)
            ordinal = self._register_claim(
                source,
                manifest,
                discovery_generation=discovery.generation,
                discovery_offset=discovery.next_offset,
            )
        except BaseException:
            shutil.rmtree(claim_root)
            raise
        self._reconcile_claim(source, claim_root)
        if flush and discovery.sweep_complete:
            marker.unlink(missing_ok=True)
        return _Admission(
            claim_root,
            ordinal,
            discovery.entries_examined,
            discovery.sweep_complete,
        )

    def _prepared_provenance(
        self,
        source: SourceConfig,
        path: Path,
        relative: str,
    ) -> tuple[dict[str, object], dict[str, bytes]]:
        observer = (
            self._provenance_observer_factory()
            if self._provenance_observer_factory is not None
            else None
        )
        if source.provenance == "capture" and observer is None:
            raise FtpAdapterError("configured provenance observer is unavailable")
        prepared = prepare_file_provenance(
            path,
            relative_path=relative,
            host_id=self.config.host_id,
            agent_name="riverhog-ftp-adapter",
            agent_version="1.0.0",
            observer=observer,
            omit_reason=(
                source.provenance_omission_reason if source.provenance == "omit" else None
            ),
        )
        return _portable_binding(prepared.binding), prepared.journals

    def _persist_journals(self, claim_root: Path, journals: Mapping[str, bytes]) -> dict[str, str]:
        result: dict[str, str] = {}
        root = claim_root / "provenance" / "journals"
        for journal_id, content in sorted(journals.items()):
            name = hashlib.sha256(journal_id.encode()).hexdigest() + ".json-seq"
            _write_atomic(root / name, bytes(content))
            result[journal_id] = f"provenance/journals/{name}"
        return result

    def _reconcile_claim(self, source: SourceConfig, claim_root: Path) -> dict[str, object]:
        manifest = _read_manifest(claim_root)
        if manifest["source"] != source.id:
            raise ClaimCollision("claim source differs from configured source")
        payload_root = claim_root / "payload"
        for row in _file_rows(manifest):
            relative = str(row["path"])
            original = Path(str(row["original"]))
            destination = payload_root / relative
            destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            original_exists = original.exists()
            destination_exists = destination.exists()
            if original_exists and destination_exists:
                if not os.path.samefile(original, destination):
                    raise ClaimCollision(f"claim has two payloads for {relative}")
                original.unlink()
                original_exists = False
            if original_exists:
                current = original.stat(follow_symlinks=False)
                _require_identity(current, row, relative)
                os.rename(original, destination)
            elif not destination_exists:
                raise SourceChanged(f"claimed payload is missing: {relative}")
            current = destination.stat(follow_symlinks=False)
            _require_identity(current, row, relative)
            if _sha256_path(destination) != row["sha256"]:
                raise SourceChanged(f"claimed payload digest changed: {relative}")
            sidecar = canonical_sidecar_path(original)
            if sidecar.is_file():
                sidecar_root = claim_root / "provenance" / "source-sidecars"
                sidecar_destination = sidecar_root / relative
                sidecar_destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                os.rename(sidecar, sidecar_destination)
        return manifest

    def _publish_claim(self, source: SourceConfig, claim_root: Path) -> ProducedCollection:
        durable_receipt = self._durable_receipt(source, claim_root.name)
        if durable_receipt is not None:
            shutil.rmtree(claim_root, ignore_errors=True)
            self._forget_claim(source, claim_root.name)
            return durable_receipt
        manifest = self._reconcile_claim(source, claim_root)
        files = tuple(
            ProducerFile(
                claim_root / "payload" / str(row["path"]),
                str(row["path"]),
                provenance=_producer_provenance(row),
            )
            for row in _file_rows(manifest)
        )
        journals = {
            journal_id: (claim_root / relative).read_bytes()
            for journal_id, relative in _mapping(manifest.get("journals"), "claim journals").items()
        }
        producer = CollectionProducer(
            self.api,
            producer_app="riverhog-ftp-adapter/v1",
            adapter_id="ftp/v1",
            adapter_version="1.0.0",
            ingest_source=source.ingest_source,
            archive_store=source.archive_store,
            description=source.description,
            tags=source.tags,
            provenance_mode="captured" if source.provenance == "capture" else "omitted",
            provenance_omission_reason=(
                source.provenance_omission_reason
                or "Protocol source explicitly omitted host provenance."
            ),
        )
        receipt = producer.publish(
            files,
            source_event_id=str(manifest["source_event_id"]),
            source_context={"adapter": "ftp", "source": source.id},
            provenance_journals=journals.items(),
            idempotency_key=str(manifest["claim_id"]),
            event_context={"adapter": "ftp", "source": source.id},
        )
        receipt_payload = {
            "format": "riverhog-ftp-adapter-receipt/v1",
            "claim_id": claim_root.name,
            "source_event_id": str(manifest["source_event_id"]),
            "collection_id": receipt.collection_id,
            "archive_root_sha256": receipt.archive_root_sha256,
            "content_identity": receipt.content_identity,
            "riverhog_receipt": receipt.receipt,
        }
        _write_json(claim_root / _RECEIPT, receipt_payload)
        _write_json(self._receipt_path(source, claim_root.name), receipt_payload)
        shutil.rmtree(claim_root)
        self._forget_claim(source, claim_root.name)
        _prune_handoff_parents(source.root, manifest)
        _prune_claim_parents(source.root, manifest)
        return receipt

    def _receipt_path(self, source: SourceConfig, claim_id: str) -> Path:
        return source.root / _CONTROL_DIR / _RECEIPTS_DIR / f"{claim_id}.json"

    def _durable_receipt(
        self,
        source: SourceConfig,
        claim_id: str,
    ) -> ProducedCollection | None:
        path = self._receipt_path(source, claim_id)
        if not path.is_file():
            return None
        payload = json.loads(path.read_text(encoding="utf-8"))
        if (
            not isinstance(payload, dict)
            or payload.get("format") != "riverhog-ftp-adapter-receipt/v1"
            or payload.get("claim_id") != claim_id
        ):
            raise FtpAdapterError("invalid durable FTP adapter receipt")
        raw_receipt = payload.get("riverhog_receipt")
        if not isinstance(raw_receipt, dict):
            raise FtpAdapterError("durable FTP adapter receipt has no Riverhog receipt")
        return ProducedCollection(
            collection_id=int(payload["collection_id"]),
            archive_root_sha256=str(payload["archive_root_sha256"]),
            content_identity=str(payload["content_identity"]),
            receipt=raw_receipt,
        )


def _claim_identity(
    source_id: str,
    event_id: str,
    rows: Sequence[tuple[str, int, int, int]],
) -> str:
    payload = "\n".join(
        [
            source_id,
            event_id,
            *(f"{path}\0{size}\0{device}\0{inode}" for path, size, device, inode in rows),
        ]
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _completed_claim_identity(
    source_id: str,
    event_id: str,
    relative_path: str,
    byte_count: int,
    sha256: str,
) -> str:
    payload = "\0".join(
        (
            "riverhog-completed-protocol-file/v1",
            source_id,
            event_id,
            relative_path,
            str(byte_count),
            sha256,
        )
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _merge_journals(
    existing: Mapping[str, bytes], additional: Mapping[str, bytes]
) -> dict[str, bytes]:
    result = dict(additional)
    for journal_id, content in result.items():
        previous = existing.get(journal_id)
        if previous is not None and previous != content:
            raise ClaimCollision(f"provenance journal identity collision: {journal_id}")
    return result


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _require_identity(current: os.stat_result, row: Mapping[str, object], path: str) -> None:
    expected = (
        int(str(row["bytes"])),
        int(str(row["device"])),
        int(str(row["inode"])),
    )
    actual = (current.st_size, current.st_dev, current.st_ino)
    if actual != expected or not stat.S_ISREG(current.st_mode):
        raise SourceChanged(f"FTP adapter source changed during claim: {path}")


def _require_completion_identity(
    current: os.stat_result,
    record: CompletionRecord,
) -> None:
    if not stat.S_ISREG(current.st_mode) or (current.st_size, current.st_dev, current.st_ino) != (
        record.bytes,
        record.device,
        record.inode,
    ):
        raise SourceChanged(f"completed FTP upload differs from its durable handoff: {record.path}")


def _read_manifest(claim_root: Path) -> dict[str, object]:
    payload = json.loads((claim_root / _MANIFEST).read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("format") != "riverhog-ftp-adapter-claim/v1":
        raise FtpAdapterError(f"invalid FTP adapter claim: {claim_root}")
    return payload


def _file_rows(manifest: Mapping[str, object]) -> list[dict[str, object]]:
    value = manifest.get("files")
    if not isinstance(value, list) or not value:
        raise FtpAdapterError("FTP adapter claim has no files")
    if any(not isinstance(item, dict) for item in value):
        raise FtpAdapterError("FTP adapter claim file entry is invalid")
    return [dict(item) for item in value]


def _mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise FtpAdapterError(f"{label} must be an object")
    return {str(key): item for key, item in value.items()}


def _producer_provenance(file_row: Mapping[str, object]) -> dict[str, object]:
    """Project a portable binding after verifying its immutable file identity."""

    binding = _mapping(file_row.get("provenance"), "claim provenance")
    status = str(binding.get("status") or "")
    expected = {"path", "bytes", "sha256", "status"}
    if status == "captured":
        expected |= {"journal_id", "current_state_id"}
    elif status == "omitted":
        expected.add("omission_reason")
    else:
        raise FtpAdapterError("claim provenance status is invalid")
    if set(binding) != expected:
        raise FtpAdapterError("claim provenance binding has unexpected fields")
    identity = (
        str(binding["path"]),
        int(str(binding["bytes"])),
        str(binding["sha256"]),
    )
    declared = (
        str(file_row["path"]),
        int(str(file_row["bytes"])),
        str(file_row["sha256"]),
    )
    if identity != declared:
        raise FtpAdapterError("claim provenance binding differs from its payload")
    if status == "captured":
        return {
            "status": "captured",
            "journal_id": str(binding["journal_id"]),
            "current_state_id": str(binding["current_state_id"]),
        }
    return {
        "status": "omitted",
        "omission_reason": str(binding["omission_reason"]),
    }


def _portable_binding(value: FileProvenanceBinding) -> dict[str, object]:
    row: dict[str, object] = {
        "path": value.path,
        "bytes": value.bytes,
        "sha256": value.sha256,
        "status": value.status,
    }
    if value.status == "captured":
        row.update(journal_id=value.journal_id, current_state_id=value.current_state_id)
    else:
        row["omission_reason"] = value.omission_reason
    return row


def _state_value(connection: sqlite3.Connection, key: str) -> str | None:
    row = connection.execute(
        "SELECT value FROM adapter_state WHERE key = ?",
        (key,),
    ).fetchone()
    return None if row is None else str(row[0])


def _set_state_value(connection: sqlite3.Connection, key: str, value: str) -> None:
    connection.execute(
        "INSERT INTO adapter_state(key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )


def _write_json(path: Path, payload: Mapping[str, object]) -> None:
    _write_atomic(
        path,
        (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode(),
    )


def _write_atomic(path: Path, content: bytes) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.part")
    with temporary.open("wb") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _prune_claim_parents(root: Path, manifest: Mapping[str, object]) -> None:
    candidates = {
        parent
        for row in _file_rows(manifest)
        for parent in (root / str(row["path"])).parents
        if parent != root and root in parent.parents
    }
    for path in sorted(candidates, key=lambda item: len(item.parts), reverse=True):
        try:
            path.rmdir()
        except OSError:
            pass


def _prune_handoff_parents(root: Path, manifest: Mapping[str, object]) -> None:
    handoffs_root = root / _CONTROL_DIR / "handoffs"
    for row in _file_rows(manifest):
        original = Path(str(row["original"]))
        try:
            relative = original.relative_to(handoffs_root)
        except ValueError:
            continue
        if not relative.parts:
            continue
        event_root = handoffs_root / relative.parts[0]
        try:
            event_root.rmdir()
        except OSError:
            pass


__all__ = [
    "ClaimCollision",
    "FtpAdapter",
    "FtpAdapterError",
    "SourceChanged",
]
