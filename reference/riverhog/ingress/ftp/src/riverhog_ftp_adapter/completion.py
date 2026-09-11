"""Durable success-qualified handoff from the FTP listener to its adapter."""

from __future__ import annotations

import builtins
import fcntl
import hashlib
import json
import os
import stat
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path, PurePosixPath

from riverhog_provenance import SIDECAR_SUFFIX, canonical_sidecar_path

CONTROL_DIR = ".riverhog-ftp-adapter"
COMPLETION_LOG = "completed-transfers.log"
COMPLETION_LOG_HEADER = "riverhog-ftp-completion-log/v1"
MAX_COMPLETION_RECORD_BYTES = 16 * 1024
_HANDOFFS_DIR = "handoffs"
_INTENTS_DIR = "handoff-intents"
_PENDING_SIDECARS_DIR = "pending-sidecars"
_INTENT_FORMAT = "riverhog-ftp-completion-intent/v1"
_PENDING_SIDECAR_FORMAT = "riverhog-ftp-pending-provenance-sidecar/v1"
_RECORD_FORMAT = "riverhog-ftp-completion-record/v1"


class CompletionError(RuntimeError):
    """The listener could not durably transfer completed bytes into custody."""


@dataclass(frozen=True, slots=True)
class CompletionRecord:
    format: str
    event_id: str
    source_id: str
    path: str
    custody: str
    bytes: int
    device: int
    inode: int

    def payload(self) -> dict[str, object]:
        return asdict(self)

    def canonical_bytes(self) -> builtins.bytes:
        return (
            json.dumps(self.payload(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            + "\n"
        ).encode("utf-8")


def completion_log_path(source_root: Path) -> Path:
    return source_root / CONTROL_DIR / COMPLETION_LOG


def initialize_completion_authority(source_root: Path) -> tuple[str, int]:
    control = source_root / CONTROL_DIR
    control.mkdir(mode=0o700, parents=True, exist_ok=True)
    (control / _HANDOFFS_DIR).mkdir(mode=0o700, exist_ok=True)
    (control / _INTENTS_DIR).mkdir(mode=0o700, exist_ok=True)
    (control / _PENDING_SIDECARS_DIR).mkdir(mode=0o700, exist_ok=True)
    path = completion_log_path(source_root)
    header = f"{COMPLETION_LOG_HEADER} {uuid.uuid4()}\n".encode("ascii")
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        pass
    else:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(header)
            stream.flush()
            os.fsync(stream.fileno())
        _fsync_directory(control)
    return read_completion_header(path)


def read_completion_header(path: Path) -> tuple[str, int]:
    with path.open("rb") as stream:
        raw = stream.readline(256)
    try:
        prefix, generation = raw.decode("ascii").rstrip("\n").split(" ", 1)
    except (UnicodeDecodeError, ValueError) as exc:
        raise CompletionError("FTP completion log header is invalid") from exc
    if prefix != COMPLETION_LOG_HEADER or str(uuid.UUID(generation)) != generation:
        raise CompletionError("FTP completion log authority is invalid")
    return generation, len(raw)


def parse_completion_record(raw: bytes) -> CompletionRecord:
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, ValueError) as exc:
        raise CompletionError("FTP completion record is not canonical JSON") from exc
    if not isinstance(payload, dict) or set(payload) != {
        "format",
        "event_id",
        "source_id",
        "path",
        "custody",
        "bytes",
        "device",
        "inode",
    }:
        raise CompletionError("FTP completion record has unexpected fields")
    record = CompletionRecord(**payload)
    if record.format != _RECORD_FORMAT:
        raise CompletionError("FTP completion record format is invalid")
    try:
        if str(uuid.UUID(record.event_id)) != record.event_id:
            raise ValueError
    except (ValueError, AttributeError) as exc:
        raise CompletionError("FTP completion event identity is invalid") from exc
    if not isinstance(record.source_id, str) or not record.source_id:
        raise CompletionError("FTP completion source identity is invalid")
    _validate_relative(record.path, "path")
    _validate_relative(record.custody, "custody")
    if not record.custody.startswith(f"{CONTROL_DIR}/{_HANDOFFS_DIR}/{record.event_id}/"):
        raise CompletionError("FTP completion custody path is not event-bound")
    for name, value in {
        "bytes": record.bytes,
        "device": record.device,
        "inode": record.inode,
    }.items():
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise CompletionError(f"FTP completion {name} is invalid")
    if record.canonical_bytes() != raw:
        raise CompletionError("FTP completion record is not canonical")
    return record


class CompletionHandoff:
    """Move exact closed upload bytes into hidden custody before FTP success."""

    def __init__(self, source_root: Path, source_id: str) -> None:
        self.source_root = source_root.resolve()
        self.source_id = source_id
        initialize_completion_authority(self.source_root)
        self.recover()

    def complete(self, uploaded_path: Path) -> CompletionRecord:
        source = uploaded_path.resolve()
        relative = _relative_source_path(self.source_root, source)
        observed = source.stat(follow_symlinks=False)
        if not stat.S_ISREG(observed.st_mode):
            raise CompletionError("completed FTP upload is not a regular file")
        _fsync_file(source)
        event_id = str(uuid.uuid4())
        custody_relative = f"{CONTROL_DIR}/{_HANDOFFS_DIR}/{event_id}/payload"
        record = CompletionRecord(
            format=_RECORD_FORMAT,
            event_id=event_id,
            source_id=self.source_id,
            path=relative,
            custody=custody_relative,
            bytes=observed.st_size,
            device=observed.st_dev,
            inode=observed.st_ino,
        )
        intent = self._intent_path(event_id)
        _write_atomic(intent, _intent_bytes(record))
        self._finish_intent(record, intent)
        return record

    def has_pending_sidecar(self, uploaded_path: Path) -> bool:
        """Report whether this sidecar pathname already awaits its payload."""

        relative = _relative_source_path(self.source_root, uploaded_path.resolve())
        if not relative.endswith(SIDECAR_SUFFIX):
            return False
        return self._pending_sidecar_path(relative).is_file()

    def recover(self) -> None:
        intent_root = self.source_root / CONTROL_DIR / _INTENTS_DIR
        disappeared_intents: set[str] = set()
        for intent in sorted(intent_root.glob("*.json"), key=lambda item: item.name):
            try:
                record = _parse_intent(intent.read_bytes())
                if record.source_id != self.source_id:
                    raise CompletionError("FTP completion intent names another source")
            except FileNotFoundError:
                # The adapter may atomically acquire an already-published
                # intent after directory enumeration but before this read.
                disappeared_intents.add(intent.stem)
                continue
            except (OSError, ValueError, TypeError, CompletionError) as exc:
                raise CompletionError(f"invalid FTP completion intent: {intent.name}") from exc
            self._finish_intent(record, intent)
        acquisition_root = self.source_root / CONTROL_DIR / _HANDOFFS_DIR
        acquired_events: set[str] = set()
        for acquisition in sorted(
            acquisition_root.glob("*/acquired-*.json"),
            key=lambda item: item.as_posix(),
        ):
            try:
                record = _parse_intent(acquisition.read_bytes())
                if record.source_id != self.source_id:
                    raise CompletionError("FTP handoff acquisition names another source")
                expected_root = (self.source_root / record.custody).parent
                if acquisition.parent != expected_root:
                    raise CompletionError("FTP handoff acquisition is not event-bound")
                if len(_exact_acquisitions(self.source_root, record)) != 1:
                    raise CompletionError("FTP handoff has multiple outstanding acquisitions")
                owner_id = acquisition.stem.removeprefix("acquired-")
                release_handoff_acquisition(self.source_root, record, owner_id)
                acquired_events.add(record.event_id)
            except FileNotFoundError:
                continue
            except (OSError, ValueError, TypeError, CompletionError) as exc:
                raise CompletionError(
                    f"invalid FTP handoff acquisition: {acquisition.name}"
                ) from exc
        if not disappeared_intents.issubset(acquired_events):
            raise CompletionError("FTP completion intent disappeared without exact acquisition")

    def _intent_path(self, event_id: str) -> Path:
        return self.source_root / CONTROL_DIR / _INTENTS_DIR / f"{event_id}.json"

    def _finish_intent(self, record: CompletionRecord, intent: Path) -> None:
        source = self.source_root / record.path
        custody = self.source_root / record.custody
        if not intent.exists():
            self._require_exact_acquisition(record)
            return
        source_exists = source.exists()
        custody_exists = custody.exists()
        if source_exists and custody_exists:
            if not record.path.endswith(SIDECAR_SUFFIX) or not os.path.samefile(source, custody):
                raise CompletionError("FTP completion intent has two payloads")
        if source_exists:
            _require_identity(source, record)
            if not custody_exists:
                custody.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                os.rename(source, custody)
                _fsync_directory(source.parent)
                _fsync_directory(custody.parent)
        elif not custody_exists:
            if not intent.exists():
                self._require_exact_acquisition(record)
                return
            raise CompletionError("FTP completion intent payload is missing")
        try:
            _require_identity(custody, record)
        except FileNotFoundError:
            if not intent.exists():
                self._require_exact_acquisition(record)
                return
            raise
        custody.chmod(0o400)
        if record.path.endswith(SIDECAR_SUFFIX):
            self._publish_pending_sidecar(record, source, custody)
        else:
            self._adopt_pending_sidecar(record, source, custody)
        self._append_record(record)
        self._retire_intent(record, intent)

    def _retire_intent(self, record: CompletionRecord, intent: Path) -> None:
        try:
            intent.unlink()
        except FileNotFoundError:
            self._require_exact_acquisition(record)
        _fsync_directory(intent.parent)

    def _require_exact_acquisition(self, record: CompletionRecord) -> None:
        acquisitions = _exact_acquisitions(self.source_root, record)
        if len(acquisitions) != 1:
            raise CompletionError("FTP completion intent disappeared without exact acquisition")
        acquisition = acquisitions[0]
        owner_id = acquisition.stem.removeprefix("acquired-")
        release_handoff_acquisition(self.source_root, record, owner_id)

    def _pending_sidecar_path(self, relative: str) -> Path:
        name = hashlib.sha256(relative.encode("utf-8")).hexdigest() + ".json"
        return self.source_root / CONTROL_DIR / _PENDING_SIDECARS_DIR / name

    def _publish_pending_sidecar(
        self,
        record: CompletionRecord,
        source: Path,
        custody: Path,
    ) -> None:
        pointer = self._pending_sidecar_path(record.path)
        if pointer.exists():
            existing = _read_pending_sidecar(pointer)
            if existing != record:
                raise CompletionError("a completed provenance sidecar already awaits this payload")
        else:
            _write_atomic(pointer, _pending_sidecar_bytes(record))
        if source.exists():
            if not os.path.samefile(source, custody):
                raise CompletionError("pending provenance sidecar pathname was replaced")
        else:
            os.link(custody, source)
            _fsync_directory(source.parent)

    def _adopt_pending_sidecar(
        self,
        record: CompletionRecord,
        source: Path,
        custody: Path,
    ) -> None:
        sidecar_relative = record.path + SIDECAR_SUFFIX
        pointer = self._pending_sidecar_path(sidecar_relative)
        if not pointer.is_file():
            return
        sidecar_record = _read_pending_sidecar(pointer)
        if sidecar_record.path != sidecar_relative:
            raise CompletionError("pending provenance sidecar names another payload")
        old_custody = self.source_root / sidecar_record.custody
        visible = canonical_sidecar_path(source)
        destination = canonical_sidecar_path(custody)
        owner_id = hashlib.sha256(record.event_id.encode("ascii")).hexdigest()
        acquisition = acquire_handoff(self.source_root, sidecar_record, owner_id)
        if destination.exists():
            _require_identity(destination, sidecar_record)
        else:
            origin = old_custody if old_custody.exists() else visible
            _require_identity(origin, sidecar_record)
            os.link(origin, destination)
            _fsync_directory(destination.parent)
        if visible.exists() and os.path.samefile(visible, destination):
            visible.unlink()
            _fsync_directory(visible.parent)
        if old_custody.exists() and os.path.samefile(old_custody, destination):
            old_parent = old_custody.parent
            old_custody.unlink()
            _fsync_directory(old_parent)
            try:
                old_parent.rmdir()
            except OSError:
                pass
        pointer.unlink()
        _fsync_directory(pointer.parent)
        if acquisition is not None:
            release_handoff_acquisition(
                self.source_root,
                sidecar_record,
                owner_id,
            )

    def _append_record(self, record: CompletionRecord) -> None:
        raw = record.canonical_bytes()
        if len(raw) > MAX_COMPLETION_RECORD_BYTES:
            raise CompletionError("FTP completion record exceeds its protocol bound")
        path = completion_log_path(self.source_root)
        with path.open("ab") as stream:
            fcntl.lockf(stream.fileno(), fcntl.LOCK_EX)
            try:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            finally:
                fcntl.lockf(stream.fileno(), fcntl.LOCK_UN)


def _intent_bytes(record: CompletionRecord) -> bytes:
    payload = {"intent_format": _INTENT_FORMAT, **asdict(record)}
    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")


def _parse_intent(raw: bytes) -> CompletionRecord:
    payload = json.loads(raw)
    if not isinstance(payload, dict) or payload.pop("intent_format", None) != _INTENT_FORMAT:
        raise ValueError
    record = CompletionRecord(**payload)
    parse_completion_record(record.canonical_bytes())
    if _intent_bytes(record) != raw:
        raise CompletionError("FTP completion intent is not canonical")
    return record


def _pending_sidecar_bytes(record: CompletionRecord) -> bytes:
    payload = {"pending_format": _PENDING_SIDECAR_FORMAT, **asdict(record)}
    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")


def _read_pending_sidecar(path: Path) -> CompletionRecord:
    try:
        payload = json.loads(path.read_bytes())
        if (
            not isinstance(payload, dict)
            or payload.pop("pending_format", None) != _PENDING_SIDECAR_FORMAT
        ):
            raise ValueError
        record = CompletionRecord(**payload)
        parse_completion_record(record.canonical_bytes())
    except (OSError, ValueError, TypeError, CompletionError) as exc:
        raise CompletionError("pending provenance sidecar authority is invalid") from exc
    return record


def acquire_handoff(
    source_root: Path,
    record: CompletionRecord,
    owner_id: str,
) -> Path | None:
    """Atomically transfer an outstanding listener intent to its exact consumer."""

    _validate_owner_id(owner_id)
    parse_completion_record(record.canonical_bytes())
    root = source_root.resolve()
    intent = root / CONTROL_DIR / _INTENTS_DIR / f"{record.event_id}.json"
    acquisition = (root / record.custody).parent / f"acquired-{owner_id}.json"
    expected = _intent_bytes(record)
    if acquisition.exists():
        if intent.exists():
            raise CompletionError("FTP handoff has two outstanding owners")
        if acquisition.read_bytes() != expected:
            raise CompletionError("FTP handoff acquisition differs from its exact intent")
        return acquisition
    try:
        os.rename(intent, acquisition)
    except FileNotFoundError as exc:
        if acquisition.exists():
            if acquisition.read_bytes() != expected:
                raise CompletionError(
                    "FTP handoff acquisition differs from its exact intent"
                ) from exc
            return acquisition
        return None
    _fsync_directory(intent.parent)
    _fsync_directory(acquisition.parent)
    return acquisition


def _exact_acquisitions(source_root: Path, record: CompletionRecord) -> list[Path]:
    event_root = (source_root.resolve() / record.custody).parent
    expected = _intent_bytes(record)
    acquisitions: list[Path] = []
    for acquisition in sorted(event_root.glob("acquired-*.json"), key=lambda item: item.name):
        _validate_owner_id(acquisition.stem.removeprefix("acquired-"))
        if acquisition.read_bytes() != expected:
            raise CompletionError("FTP handoff acquisition differs from its exact intent")
        acquisitions.append(acquisition)
    return acquisitions


def release_handoff_acquisition(
    source_root: Path,
    record: CompletionRecord,
    owner_id: str,
) -> None:
    """Retire exact consumer evidence after its replacement custody is durable."""

    _validate_owner_id(owner_id)
    event_root = (source_root.resolve() / record.custody).parent
    if not event_root.exists():
        return
    acquisition = event_root / f"acquired-{owner_id}.json"
    if acquisition.exists() and acquisition.read_bytes() != _intent_bytes(record):
        raise CompletionError("FTP handoff acquisition differs from its exact intent")
    acquisition.unlink(missing_ok=True)
    _fsync_directory(event_root)
    try:
        event_root.rmdir()
    except OSError:
        pass


def _validate_owner_id(value: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise CompletionError("FTP handoff owner identity is invalid")


def _relative_source_path(root: Path, path: Path) -> str:
    try:
        relative = path.relative_to(root).as_posix()
    except ValueError as exc:
        raise CompletionError("FTP upload is outside its configured source") from exc
    _validate_relative(relative, "path")
    if CONTROL_DIR in PurePosixPath(relative).parts:
        raise CompletionError("FTP upload uses the adapter control namespace")
    return relative


def _validate_relative(value: object, label: str) -> None:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise CompletionError(f"FTP completion {label} is invalid")
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or path.as_posix() != value
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise CompletionError(f"FTP completion {label} is not canonical relative POSIX")


def _require_identity(path: Path, record: CompletionRecord) -> None:
    observed = path.stat(follow_symlinks=False)
    if not stat.S_ISREG(observed.st_mode) or (
        observed.st_size,
        observed.st_dev,
        observed.st_ino,
    ) != (record.bytes, record.device, record.inode):
        raise CompletionError("FTP completion payload differs from its durable identity")


def _fsync_file(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_atomic(path: Path, content: bytes) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4()}.part")
    with temporary.open("xb") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    _fsync_directory(path.parent)


__all__ = [
    "COMPLETION_LOG_HEADER",
    "CONTROL_DIR",
    "CompletionError",
    "CompletionHandoff",
    "CompletionRecord",
    "MAX_COMPLETION_RECORD_BYTES",
    "acquire_handoff",
    "completion_log_path",
    "initialize_completion_authority",
    "parse_completion_record",
    "read_completion_header",
    "release_handoff_acquisition",
]
