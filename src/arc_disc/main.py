from __future__ import annotations

import hashlib
import importlib
import os
import time
from dataclasses import dataclass, field
from typing import Annotated, Any

import typer

from arc_cli.client import ApiClient
from arc_cli.output import emit
from arc_core.domain.errors import ArcError

app = typer.Typer(help="arc optical recovery CLI")


@app.callback()
def arc_disc_app() -> None:
    """Keep the CLI in group mode so `arc-disc fetch ...` stays canonical."""


class PlaceholderOpticalReader:
    def read(self, disc_path: str, *, device: str) -> bytes:
        raise NotImplementedError(f"optical read not implemented for {disc_path} on {device}")


class PlaceholderCrypto:
    def decrypt_entry(self, encrypted: bytes, enc: dict[str, Any]) -> bytes:
        raise NotImplementedError("entry decryption is not implemented")


@dataclass(frozen=True, slots=True)
class RecoveryCopyHint:
    copy_id: str
    location: str
    disc_path: str
    enc: dict[str, Any]


@dataclass(frozen=True, slots=True)
class RecoveryPartHint:
    index: int
    bytes: int
    sha256: str
    copies: tuple[RecoveryCopyHint, ...]


@dataclass(frozen=True, slots=True)
class RecoveryEntry:
    id: str
    path: str
    bytes: int
    sha256: str
    parts: tuple[RecoveryPartHint, ...]

def _load_factory(spec: str) -> object:
    module_name, sep, attr_name = spec.partition(":")
    if not sep:
        raise RuntimeError(f"invalid factory spec: {spec!r}")
    factory = getattr(importlib.import_module(module_name), attr_name)
    if not callable(factory):
        raise RuntimeError(f"factory must be callable: {spec!r}")
    return factory()


def build_optical_reader() -> object:
    spec = os.getenv("ARC_DISC_READER_FACTORY")
    if spec:
        return _load_factory(spec)
    return PlaceholderOpticalReader()


def build_crypto() -> object:
    spec = os.getenv("ARC_DISC_CRYPTO_FACTORY")
    if spec:
        return _load_factory(spec)
    return PlaceholderCrypto()


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _copy_from_manifest(payload: dict[str, Any]) -> RecoveryCopyHint:
    return RecoveryCopyHint(
        copy_id=str(payload["copy"]),
        location=str(payload["location"]),
        disc_path=str(payload["disc_path"]),
        enc=dict(payload["enc"]),
    )


def _part_from_manifest(payload: dict[str, Any]) -> RecoveryPartHint:
    copies = tuple(_copy_from_manifest(copy) for copy in payload.get("copies", []))
    if not copies:
        raise RuntimeError("fetch manifest part is missing copy hints")
    return RecoveryPartHint(
        index=int(payload["index"]),
        bytes=int(payload["bytes"]),
        sha256=str(payload["sha256"]),
        copies=copies,
    )


def _entry_from_manifest(payload: dict[str, Any]) -> RecoveryEntry:
    manifest_parts = payload.get("parts")
    if manifest_parts:
        parts = tuple(
            _part_from_manifest(part)
            for part in sorted(manifest_parts, key=lambda item: int(item["index"]))
        )
    else:
        copies = tuple(_copy_from_manifest(copy) for copy in payload.get("copies", []))
        if not copies:
            raise RuntimeError(f"fetch manifest entry is missing copy hints: {payload['id']}")
        parts = (
            RecoveryPartHint(
                index=0,
                bytes=int(payload["bytes"]),
                sha256=str(payload["sha256"]),
                copies=copies,
            ),
        )
    return RecoveryEntry(
        id=str(payload["id"]),
        path=str(payload["path"]),
        bytes=int(payload["bytes"]),
        sha256=str(payload["sha256"]),
        parts=parts,
    )


def _validate_part(
    entry: RecoveryEntry,
    part: RecoveryPartHint,
    plaintext: bytes,
) -> None:
    if len(plaintext) != part.bytes or _sha256_bytes(plaintext) != part.sha256:
        raise RuntimeError(
            f"recovered part {part.index} for {entry.path} did not match the fetch manifest"
        )


def _prompt_for_disc(copy: RecoveryCopyHint, *, device: str) -> None:
    typer.echo(
        (
            f"Insert disc {copy.copy_id} from {copy.location} into {device}, "
            "then press Enter to continue."
        ),
        err=True,
    )
    try:
        input()
    except EOFError as exc:  # pragma: no cover - exercised via subprocess acceptance tests
        raise RuntimeError("stdin closed while waiting for disc insertion") from exc


@dataclass(slots=True)
class ProgressReporter:
    entries: tuple[RecoveryEntry, ...]
    started_at: float
    recovered_bytes_by_entry: dict[str, int] = field(default_factory=dict)
    recovered_manifest_bytes: int = 0

    @classmethod
    def begin(cls, entries: tuple[RecoveryEntry, ...]) -> ProgressReporter:
        return cls(entries=entries, started_at=time.monotonic())

    @property
    def manifest_total_bytes(self) -> int:
        return sum(entry.bytes for entry in self.entries)

    def record_part(self, entry: RecoveryEntry, part: RecoveryPartHint) -> None:
        self.recovered_bytes_by_entry[entry.id] = self.recovered_bytes_by_entry.get(entry.id, 0) + part.bytes
        self.recovered_manifest_bytes += part.bytes

    def report(self, entry: RecoveryEntry) -> None:
        entry_total = max(entry.bytes, 1)
        manifest_total = max(self.manifest_total_bytes, 1)
        entry_percent = (self.recovered_bytes_by_entry.get(entry.id, 0) / entry_total) * 100
        manifest_percent = (self.recovered_manifest_bytes / manifest_total) * 100
        elapsed = max(time.monotonic() - self.started_at, 0.001)
        rate = self.recovered_manifest_bytes / elapsed
        typer.echo(
            (
                f"current file {entry.path}: {entry_percent:.1f}% | "
                f"manifest: {manifest_percent:.1f}% | rate: {rate:.1f} B/s"
            ),
            err=True,
        )


def _recover_pending_parts(
    entries: tuple[RecoveryEntry, ...],
    *,
    reader: Any,
    crypto: Any,
    device: str,
    progress: ProgressReporter,
) -> dict[str, dict[int, bytes]]:
    pending_by_copy: dict[
        str,
        tuple[RecoveryCopyHint, list[tuple[RecoveryEntry, RecoveryPartHint]]],
    ] = {}
    copy_order: list[str] = []
    recovered_parts: dict[str, dict[int, bytes]] = {}

    for entry in entries:
        for part in entry.parts:
            copy = part.copies[0]
            bucket = pending_by_copy.get(copy.copy_id)
            if bucket is None:
                bucket = (copy, [])
                pending_by_copy[copy.copy_id] = bucket
                copy_order.append(copy.copy_id)
            bucket[1].append((entry, part))

    for copy_id in copy_order:
        copy, items = pending_by_copy[copy_id]
        _prompt_for_disc(copy, device=device)
        for entry, part in items:
            encrypted = reader.read(copy.disc_path, device=device)
            plaintext = crypto.decrypt_entry(encrypted, copy.enc)
            _validate_part(entry, part, plaintext)
            recovered_parts.setdefault(entry.id, {})[part.index] = plaintext
            progress.record_part(entry, part)
            progress.report(entry)
    return recovered_parts


def _reconstruct_entry(recovered_parts: dict[str, dict[int, bytes]], entry: RecoveryEntry) -> bytes:
    parts: list[bytes] = []
    entry_parts = recovered_parts.get(entry.id, {})
    for part in entry.parts:
        plaintext = entry_parts.get(part.index)
        if plaintext is None:
            raise RuntimeError(f"missing recovered part {part.index} for {entry.path}")
        parts.append(plaintext)
    plaintext = b"".join(parts)
    if len(plaintext) != entry.bytes or _sha256_bytes(plaintext) != entry.sha256:
        raise RuntimeError(
            f"reconstructed plaintext for {entry.path} did not match the fetch manifest"
        )
    return plaintext


@app.command("fetch")
def fetch_cmd(
    fetch_id: Annotated[str, typer.Argument(help="Fetch id")],
    device: Annotated[str, typer.Option("--device", help="Optical device path")] = "/dev/sr0",
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    try:
        client = ApiClient()
        manifest = client.get_fetch_manifest(fetch_id)
        reader = build_optical_reader()
        crypto = build_crypto()
        entries = tuple(_entry_from_manifest(entry) for entry in manifest.get("entries", []))
        progress = ProgressReporter.begin(entries)

        recovered_parts = _recover_pending_parts(
            entries,
            reader=reader,
            crypto=crypto,
            device=device,
            progress=progress,
        )

        for entry in entries:
            plaintext = _reconstruct_entry(recovered_parts, entry)
            client.upload_fetch_entry(fetch_id, entry.id, entry.sha256, plaintext)
            progress.report(entry)

        payload = client.complete_fetch(fetch_id)
    except (ArcError, RuntimeError) as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    emit(payload, json_mode=json_mode)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
