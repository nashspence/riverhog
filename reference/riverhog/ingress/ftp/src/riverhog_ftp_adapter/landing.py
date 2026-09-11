"""Restartable finalized-receipt custody transfer for the FTP intake directory."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import threading
import time
from bisect import bisect_right
from collections.abc import Generator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from riverhog_client import ApiClient
from riverhog_client.producer import CollectionProducer, ProducedCollection, ProducerFile
from riverhog_provenance import (
    FileProvenanceBinding,
    FileStateObserverFactory,
    canonical_sidecar_path,
    prepare_file_provenance,
)

from riverhog_ftp_adapter.config import FtpAdapterConfig, SourceConfig

_CONTROL_DIR = ".riverhog-ftp-adapter"
_FLUSH_MARKER = ".riverhog-ftp-flush"
_MANIFEST = "claim.json"
_RECEIPT = "receipt.json"
_RECEIPTS_DIR = "receipts"
_RECONCILE_CURSOR = "reconcile-cursor.json"


@dataclass(frozen=True, slots=True)
class _Admission:
    claim_root: Path | None
    entries_examined: int
    sweep_complete: bool


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
            source.root.mkdir(mode=0o700, parents=True, exist_ok=True)
            claims = self._claim_roots(source)
            capacity_available = len(claims) < self.config.pending_claim_capacity
            retry_budget = self.config.claim_attempt_budget - int(capacity_available)
            work = self._claim_work(source, claims, limit=retry_budget)
            source_completed = 0
            source_failed = 0
            attempts = 0
            for claim_root in work:
                try:
                    self._publish_claim(source, claim_root)
                except Exception as exc:
                    failed.append(
                        {"source": source.id, "claim": claim_root.name, "error": str(exc)[:1000]}
                    )
                    source_failed += 1
                else:
                    completed += 1
                    source_completed += 1
                finally:
                    attempts += 1
                    self._save_reconcile_cursor(source, claim_root.name)
            pending_after_work = len(claims) - source_completed
            admission = _Admission(None, 0, False)
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
                            self._save_reconcile_cursor(source, admission.claim_root.name)
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
            claims = self._claim_roots(source)
            claim_bytes = sum(
                int(str(row["bytes"]))
                for claim in claims
                for row in _file_rows(_read_manifest(claim))
            )
            rows.append(
                {
                    "id": source.id,
                    "ingest_source": source.ingest_source,
                    "claims": len(claims),
                    "claim_bytes": claim_bytes,
                    "close_mode": source.close_mode,
                    "max_files": source.max_files,
                    "max_bytes": source.max_bytes,
                    "provenance": source.provenance,
                    "pending_claim_capacity": self.config.pending_claim_capacity,
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
            return durable_receipt
        if claim_root.exists():
            self._reconcile_claim(source, claim_root)
            return self._publish_claim(source, claim_root)
        if not path.exists():
            raise SourceChanged("completed protocol upload is missing and has no receipt")
        if len(self._claim_roots(source)) >= self.config.pending_claim_capacity:
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
            except BaseException:
                shutil.rmtree(claim_root)
                raise
        self._reconcile_claim(source, claim_root)
        return self._publish_claim(source, claim_root)

    def _claims_root(self, source: SourceConfig) -> Path:
        return source.root / _CONTROL_DIR / "claims"

    def _claim_roots(self, source: SourceConfig) -> list[Path]:
        root = self._claims_root(source)
        if not root.is_dir():
            return []
        claims: list[Path] = []
        with os.scandir(root) as entries:
            for entry in entries:
                path = Path(entry.path)
                if entry.is_dir(follow_symlinks=False) and (path / _MANIFEST).is_file():
                    claims.append(path)
                    if len(claims) > self.config.pending_claim_capacity:
                        raise FtpAdapterError(
                            "pending claims exceed the configured admission capacity"
                        )
        return sorted(claims)

    def _claim_work(
        self,
        source: SourceConfig,
        claims: Sequence[Path],
        *,
        limit: int,
    ) -> tuple[Path, ...]:
        if not claims or limit < 1:
            return ()
        names = [claim.name for claim in claims]
        after = self._load_reconcile_cursor(source)
        start = bisect_right(names, after) if after is not None else 0
        ordered = (*claims[start:], *claims[:start])
        return tuple(ordered[:limit])

    def _cursor_path(self, source: SourceConfig) -> Path:
        return source.root / _CONTROL_DIR / _RECONCILE_CURSOR

    def _load_reconcile_cursor(self, source: SourceConfig) -> str | None:
        path = self._cursor_path(source)
        if not path.is_file():
            return None
        value = json.loads(path.read_text(encoding="utf-8"))
        if (
            not isinstance(value, dict)
            or value.get("format") != "riverhog-ftp-adapter-reconcile-cursor/v1"
            or value.get("source") != source.id
            or not isinstance(value.get("after"), str)
        ):
            raise FtpAdapterError("invalid FTP adapter reconcile cursor")
        return str(value["after"])

    def _save_reconcile_cursor(self, source: SourceConfig, claim_id: str) -> None:
        _write_json(
            self._cursor_path(source),
            {
                "format": "riverhog-ftp-adapter-reconcile-cursor/v1",
                "source": source.id,
                "after": claim_id,
            },
        )

    def _source_entries(
        self, source: SourceConfig
    ) -> Generator[tuple[Path, str, os.stat_result | None], None, None]:
        iterators: list[Any] = [os.scandir(source.root)]
        try:
            while iterators:
                try:
                    entry = next(iterators[-1])
                except StopIteration:
                    iterators.pop().close()
                    continue
                path = Path(entry.path)
                relative = path.relative_to(source.root)
                if relative.parts and relative.parts[0] == _CONTROL_DIR:
                    yield path, relative.as_posix(), None
                    continue
                try:
                    is_directory = entry.is_dir(follow_symlinks=False)
                except FileNotFoundError:
                    yield path, relative.as_posix(), None
                    continue
                if is_directory:
                    yield path, relative.as_posix(), None
                    try:
                        iterators.append(os.scandir(path))
                    except FileNotFoundError:
                        pass
                    continue
                try:
                    observed = entry.stat(follow_symlinks=False)
                except FileNotFoundError:
                    observed = None
                yield path, relative.as_posix(), observed
        finally:
            for iterator in iterators:
                iterator.close()

    def _discover_batch(
        self, source: SourceConfig, *, flush: bool
    ) -> tuple[list[tuple[Path, str, os.stat_result]], int, bool]:
        if source.close_mode == "explicit-flush" and not flush:
            return [], 0, True
        cutoff_ns = time.time_ns() - source.stable_seconds * 1_000_000_000
        selected: list[tuple[Path, str, os.stat_result]] = []
        total = 0
        examined = 0
        iterator = self._source_entries(source)
        while examined < self.config.discovery_entry_budget:
            try:
                path, relative, observed = next(iterator)
            except StopIteration:
                selected.sort(key=lambda row: row[1])
                return selected, examined, True
            examined += 1
            if path.name == _FLUSH_MARKER or path.name.endswith(".riverhog-provenance.json-seq"):
                continue
            if ".riverhog" in Path(relative).parts:
                continue
            if observed is None or not stat.S_ISREG(observed.st_mode):
                continue
            if not flush and observed.st_mtime_ns > cutoff_ns:
                continue
            if selected and (
                len(selected) >= source.max_files or total + observed.st_size > source.max_bytes
            ):
                iterator.close()
                selected.sort(key=lambda row: row[1])
                return selected, examined, False
            selected.append((path, relative, observed))
            total += observed.st_size
        iterator.close()
        selected.sort(key=lambda row: row[1])
        return selected, examined, False

    def _claim_new_batch(self, source: SourceConfig) -> _Admission:
        marker = source.root / _FLUSH_MARKER
        flush = marker.is_file()
        selected, examined, sweep_complete = self._discover_batch(source, flush=flush)
        if not selected:
            if flush and sweep_complete:
                marker.unlink(missing_ok=True)
            return _Admission(None, examined, sweep_complete)
        event_id = hashlib.sha256(
            "\n".join(
                f"{row[1]}\0{row[2].st_size}\0{row[2].st_mtime_ns}" for row in selected
            ).encode()
        ).hexdigest()
        claim_id = _claim_identity(
            source.id,
            event_id,
            tuple(
                (relative, item.st_size, item.st_dev, item.st_ino) for _, relative, item in selected
            ),
        )
        claim_root = self._claims_root(source) / claim_id
        if claim_root.exists():
            if (claim_root / _MANIFEST).is_file():
                return _Admission(claim_root, examined, sweep_complete)
            # Finalized claim intent is the manifest. Construction can leave a
            # pre-manifest directory behind, but no payload move has begun yet.
            shutil.rmtree(claim_root)
        claim_root.mkdir(mode=0o700, parents=True)
        try:
            files: list[dict[str, object]] = []
            journals: dict[str, bytes] = {}
            for original, relative, observed in selected:
                binding, captured = self._prepared_provenance(source, original, relative)
                journals.update(_merge_journals(journals, captured))
                files.append(
                    {
                        "path": relative,
                        "bytes": observed.st_size,
                        "sha256": _sha256_path(original),
                        "device": observed.st_dev,
                        "inode": observed.st_ino,
                        "original": str(original.resolve()),
                        "provenance": binding,
                    }
                )
            manifest = {
                "format": "riverhog-ftp-adapter-claim/v1",
                "claim_id": claim_id,
                "source_event_id": event_id,
                "source": source.id,
                "files": files,
                "journals": self._persist_journals(claim_root, journals),
            }
            _write_json(claim_root / _MANIFEST, manifest)
        except BaseException:
            shutil.rmtree(claim_root)
            raise
        self._reconcile_claim(source, claim_root)
        if flush and sweep_complete:
            marker.unlink(missing_ok=True)
        return _Admission(claim_root, examined, sweep_complete)

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


__all__ = [
    "ClaimCollision",
    "FtpAdapter",
    "FtpAdapterError",
    "SourceChanged",
]
