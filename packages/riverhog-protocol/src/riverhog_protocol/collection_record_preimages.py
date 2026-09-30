"""Bounded reconstruction of exact profiled record fragments, preserving sealed bytes."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import sqlite3
from collections.abc import Iterable, Iterator, Mapping
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Self

from riverhog_canonical_json import canonical_json_bytes, require_canonical_json
from riverhog_provenance_contracts import ContractCatalog

from .collection_production_provenance import (
    COLLECTION_PRODUCTION_CONTRACT_ID,
    COLLECTION_RECORD_FRAGMENT_BYTES_MAX,
    RECORD_FRAGMENT_SCHEMA_ID,
    RECORD_MANIFEST_SCHEMA_ID,
    collection_production_contract,
)


class CollectionRecordPreimages:
    """Retain exact record bytes from one independently selected assertion scope.

    Ordering and uniqueness live in a disk index. A potentially large record is
    reconstructed as bounded chunks; nulls and numeric tokens are never parsed
    and reserialized by this transport.
    """

    def __init__(
        self, extensions: Iterable[Mapping[str, Any]], *, subject: Mapping[str, Any]
    ) -> None:
        self._scratch = TemporaryDirectory(prefix="riverhog-record-preimages-")
        self._db = sqlite3.connect(Path(self._scratch.name) / "records.sqlite3")
        self._db.execute("PRAGMA cache_size = -512")
        self._db.executescript(
            "CREATE TABLE manifests (kind TEXT PRIMARY KEY, value TEXT);"
            "CREATE TABLE fragments (kind TEXT, offset TEXT, value TEXT, "
            "PRIMARY KEY(kind, offset));"
        )
        contract = collection_production_contract()
        catalog = ContractCatalog((contract,))
        properties = {
            COLLECTION_PRODUCTION_CONTRACT_ID + "/record-manifest": RECORD_MANIFEST_SCHEMA_ID,
            COLLECTION_PRODUCTION_CONTRACT_ID + "/record-fragment": RECORD_FRAGMENT_SCHEMA_ID,
        }
        try:
            for row in extensions:
                schema_id = properties.get(row["property"])
                if schema_id is None or row["subject"] != subject:
                    continue
                if row["value"]["type"] != "json":
                    raise ValueError("required record is not a profiled JSON value")
                value = row["value"]["value"]
                if value["profile"] != {
                    "contract_id": contract.contract_id,
                    "contract_sha256": contract.contract_sha256,
                    "schema_id": schema_id,
                }:
                    raise ValueError("required record profile differs from its property")
                catalog.validate_profile(value["profile"], value["data"])
                data = value["data"]
                if schema_id == RECORD_MANIFEST_SCHEMA_ID:
                    self._db.execute(
                        "INSERT INTO manifests VALUES (?, ?)",
                        (data["record_kind"], json.dumps(data)),
                    )
                else:
                    self._db.execute(
                        "INSERT INTO fragments VALUES (?, ?, ?)",
                        (data["record_kind"], data["offset"], json.dumps(data)),
                    )
            missing = self._db.execute(
                "SELECT kind FROM fragments EXCEPT SELECT kind FROM manifests LIMIT 1"
            ).fetchone()
            if missing:
                raise ValueError("record fragments have no exact manifest")
        except sqlite3.IntegrityError as exc:
            self._db.close()
            self._scratch.cleanup()
            raise ValueError("duplicate required record manifest or fragment") from exc
        except BaseException:
            self._db.close()
            self._scratch.cleanup()
            raise

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        self._db.close()
        self._scratch.cleanup()

    def kinds(self) -> Iterator[str]:
        for (kind,) in self._db.execute("SELECT kind FROM manifests ORDER BY kind"):
            yield kind

    def identity(self, kind: str) -> tuple[str, int]:
        row = self._db.execute("SELECT value FROM manifests WHERE kind = ?", (kind,)).fetchone()
        if row is None:
            raise ValueError("required record manifest is missing")
        manifest = json.loads(row[0])
        return manifest["record_sha256"], int(manifest["total_bytes"])

    def inventory_sha256(self) -> str:
        return completion_record_inventory_sha256(
            (kind, *self.identity(kind))
            for (kind,) in self._db.execute("SELECT kind FROM manifests ORDER BY kind")
        )

    def chunks(self, kind: str) -> Iterator[bytes]:
        manifest_row = self._db.execute(
            "SELECT value FROM manifests WHERE kind = ?", (kind,)
        ).fetchone()
        if manifest_row is None:
            raise ValueError("required record manifest is missing")
        manifest = json.loads(manifest_row[0])
        offset = count = 0
        digest = hashlib.sha256()
        for (encoded,) in self._db.execute(
            "SELECT value FROM fragments WHERE kind = ? ORDER BY LENGTH(offset), offset", (kind,)
        ):
            part = json.loads(encoded)
            if (
                int(part["offset"]) != offset
                or part["record_sha256"] != manifest["record_sha256"]
                or part["total_bytes"] != manifest["total_bytes"]
            ):
                raise ValueError("required record fragments differ or are not contiguous")
            try:
                content = base64.b64decode(part["data_base64"], validate=True)
            except (binascii.Error, ValueError) as exc:
                raise ValueError("required record fragment is not exact base64") from exc
            if (
                not content
                or len(content) > COLLECTION_RECORD_FRAGMENT_BYTES_MAX
                or base64.b64encode(content).decode("ascii") != part["data_base64"]
            ):
                raise ValueError("required record fragment exceeds its bounded byte contract")
            count += 1
            offset += len(content)
            digest.update(content)
            yield content
        if (
            count != int(manifest["part_count"])
            or offset != int(manifest["total_bytes"])
            or digest.hexdigest() != manifest["record_sha256"]
        ):
            raise ValueError("required record preimage is incomplete or substituted")

    def validate(self, *, expected_kinds: Iterable[str]) -> None:
        # Declaration size is bounded independently from record and collection extent.
        if tuple(self.kinds()) != tuple(expected_kinds):
            raise ValueError("required record manifests differ from the accepted declaration")
        for kind in self.kinds():
            for _chunk in self.chunks(kind):
                pass


__all__ = ["CollectionRecordPreimages"]


def canonical_record_sequence(records: Iterable[Mapping[str, Any]]) -> Iterator[bytes]:
    """Serialize independently bounded JCS records with no total sequence ceiling."""
    for record in records:
        encoded = canonical_json_bytes(dict(record))
        if len(encoded) > 1024 * 1024:
            raise ValueError("completion sequence record exceeds its bounded message contract")
        yield b"\x1e" + encoded + b"\n"


def iter_canonical_record_sequence(chunks: Iterable[bytes]) -> Iterator[dict[str, Any]]:
    pending = bytearray()
    for chunk in chunks:
        for start in range(0, len(chunk), 128 * 1024):
            pending.extend(chunk[start : start + 128 * 1024])
            while (end := pending.find(b"\n")) != -1:
                encoded = bytes(pending[:end])
                del pending[: end + 1]
                if not encoded.startswith(b"\x1e") or len(encoded) > 1024 * 1024 + 1:
                    raise ValueError("completion sequence framing or record size is invalid")
                value: Any = require_canonical_json(encoded[1:])
                if not isinstance(value, dict):
                    raise ValueError("completion sequence record is not a canonical object")
                yield value
            if len(pending) > 1024 * 1024 + 1:
                raise ValueError("completion sequence record exceeds its bounded message contract")
    if pending:
        raise ValueError("completion sequence terminal record is incomplete")


__all__ += ["canonical_record_sequence", "iter_canonical_record_sequence"]


def completion_record_inventory_sha256(records: Iterable[tuple[str, str, int]]) -> str:
    """Commit exact sorted preimage identities before generating journal metadata."""
    digest = hashlib.sha256(b"riverhog-completion-record-inventory/v1\x00")
    previous = None
    count = 0
    for kind, sha256, size in records:
        if previous is not None and kind <= previous:
            raise ValueError("completion record inventory must be unique and ordered")
        raw = canonical_json_bytes(
            {"record_kind": kind, "record_sha256": sha256, "bytes": str(size)}
        )
        digest.update(len(raw).to_bytes(4, "big"))
        digest.update(raw)
        previous = kind
        count += 1
    if count == 0:
        raise ValueError("completion record inventory is empty")
    digest.update(b"\x00count:" + str(count).encode("ascii"))
    return digest.hexdigest()
