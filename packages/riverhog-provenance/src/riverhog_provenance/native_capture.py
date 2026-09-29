"""Internal native filesystem acquisition used by canonical source adapters.

This module retains low-level platform collection mechanics and exact raw values.
It does not define Riverhog archive members or a second provenance journal.
"""

from __future__ import annotations

import base64
import datetime as dt
import hashlib
import json
import locale
import os
import stat as statmod
import urllib.parse
import uuid
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from os import PathLike
from typing import Any

from riverhog_canonical_json import format_scalar
from time_formats import format_utc_ns

from .constants import PACKAGE_NAME, PACKAGE_VERSION, PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
from .errors import ObservationError

type PathInput = str | bytes | PathLike[str] | PathLike[bytes]
type JsonObject = dict[str, Any]

OBSERVER_NAMESPACE = uuid.UUID("99b19dc2-79dd-4c27-a097-245b3b6b7169")
DEFAULT_OBSERVER_AGENT_ID = (
    f"urn:uuid:{uuid.uuid5(OBSERVER_NAMESPACE, PACKAGE_NAME + ':' + PACKAGE_VERSION)}"
)
COVERAGE_CATEGORIES = (
    "content_fixity",
    "locator",
    "basic_filesystem",
    "timestamps",
    "ownership",
    "permissions",
    "native_identifiers",
    "extended_attributes",
    "access_control",
    "alternate_streams",
    "resource_forks",
    "file_flags",
    "security_metadata",
    "storage_layout",
    "special_file_features",
    "native_metadata_other",
)


class UnsupportedPlatformError(ObservationError):
    pass


class UnsupportedFileTypeError(ObservationError):
    pass


class SymlinkRefusedError(ObservationError):
    pass


class UnstableFileError(ObservationError):
    pass


class NativeObservationError(ObservationError):
    pass


class LargeValueDisposition(StrEnum):
    """How to retain native values larger than the inline threshold."""

    DIGEST_ONLY = "digest_only"
    NOT_RETAINED = "not_retained"
    FAIL = "fail"


@dataclass(frozen=True, slots=True)
class NativeCapturePolicy:
    """Capture policy shared by all platform observers.

    The defaults are intentionally archival rather than minimal. They capture all
    native categories that this implementation can enumerate without
    interpreting the primary file bytes.
    """

    strict_consistency: bool = True
    attempt_noatime: bool = True
    verify_path_binding: bool = True
    second_content_hash: bool = False
    hash_chunk_bytes: int = 8 * 1024 * 1024
    inline_native_value_bytes: int = 1024 * 1024
    maximum_native_value_bytes: int = 256 * 1024 * 1024
    large_value_disposition: LargeValueDisposition = LargeValueDisposition.DIGEST_ONLY
    capture_xattrs: bool = True
    capture_acl: bool = True
    capture_file_flags: bool = True
    capture_sparse_map: bool = True
    capture_special_features: bool = True
    capture_native_stat: bool = True
    resolve_principals: bool = True
    include_access_time: bool = True
    include_hostname: bool = True
    include_effective_principal: bool = True
    maximum_sparse_extents: int = 100_000
    resource_fork_chunk_bytes: int = 8 * 1024 * 1024
    native_stream_chunk_bytes: int = 8 * 1024 * 1024
    maximum_native_streams: int = 10_000
    capture_system_acl: bool = False
    windows_allow_shared_write: bool = False
    windows_allow_shared_delete: bool = False
    windows_follow_non_name_surrogate_reparse_points: bool = True
    windows_capture_usn: bool = True
    windows_capture_object_id: bool = True

    def __post_init__(self) -> None:
        positive = {
            "hash_chunk_bytes": self.hash_chunk_bytes,
            "inline_native_value_bytes": self.inline_native_value_bytes,
            "maximum_native_value_bytes": self.maximum_native_value_bytes,
            "maximum_sparse_extents": self.maximum_sparse_extents,
            "resource_fork_chunk_bytes": self.resource_fork_chunk_bytes,
            "native_stream_chunk_bytes": self.native_stream_chunk_bytes,
            "maximum_native_streams": self.maximum_native_streams,
        }
        for name, value in positive.items():
            if value <= 0:
                raise ValueError(f"{name} must be positive")
        if self.inline_native_value_bytes > self.maximum_native_value_bytes:
            raise ValueError("inline_native_value_bytes cannot exceed maximum_native_value_bytes")


@dataclass(frozen=True, slots=True)
class NativeCaptureRequest:
    """Technical options for native collection within a canonical source session."""

    host_id: str
    observer_agent_id: str | None = None
    policy: NativeCapturePolicy = field(default_factory=NativeCapturePolicy)


@dataclass(frozen=True, slots=True)
class NativeStat:
    """Cross-platform stability and basic-filesystem snapshot."""

    device: int
    inode: int
    mode: int
    nlink: int
    uid: int
    gid: int
    size: int
    atime_ns: int
    mtime_ns: int
    ctime_ns: int
    birthtime_ns: int | None = None
    blocks: int | None = None
    block_size: int | None = None
    flags: int | None = None
    generation: int | None = None
    mount_id: int | None = None
    rdev: int | None = None
    extras: Mapping[str, Any] = field(default_factory=dict)

    def stability_key(self) -> tuple[int, ...]:
        """Fields expected to remain stable while a regular-file state is read.

        Access time is deliberately excluded: reading the payload can update it on
        hosts where a non-atime descriptor cannot be obtained. The state records
        the post-read access time and emits a diagnostic where that caveat applies.
        """

        return (
            self.device,
            self.inode,
            self.mode,
            self.nlink,
            self.uid,
            self.gid,
            self.size,
            self.mtime_ns,
            self.ctime_ns,
            self.flags if self.flags is not None else -1,
            self.generation if self.generation is not None else -1,
        )


@dataclass(frozen=True, slots=True)
class NativeExtensionDraft:
    """One native detail captured before canonical profile serialization."""

    subject_role: str
    property: str
    value: JsonObject
    confidence: str = "high"
    note: str | None = None


class BoundedNativeMetadata(list[JsonObject]):
    """Limit retained native rows before they can exhaust memory or one journal entry."""

    def __init__(self, maximum_bytes: int = PROVENANCE_JOURNAL_ENTRY_BYTES_MAX // 2) -> None:
        super().__init__()
        if type(maximum_bytes) is not int or maximum_bytes < 1:
            raise ValueError("maximum_bytes must be positive")
        self.maximum_bytes = maximum_bytes
        self.encoded_bytes = 0

    def append(self, row: JsonObject) -> None:
        estimated = len(json.dumps(row, ensure_ascii=True, separators=(",", ":"), default=str))
        if self.encoded_bytes + estimated > self.maximum_bytes:
            raise NativeObservationError("native metadata exceeds bounded capture budget")
        super().append(row)
        self.encoded_bytes += estimated


@dataclass(slots=True)
class NativeCapture:
    timestamps: list[JsonObject] = field(default_factory=list)
    access: JsonObject | None = None
    native_identifiers: list[JsonObject] = field(default_factory=list)
    native_metadata: list[JsonObject] = field(default_factory=BoundedNativeMetadata)
    coverage: dict[str, str] = field(default_factory=dict)
    diagnostics: list[JsonObject] = field(default_factory=list)
    environment: JsonObject | None = None
    extension_drafts: list[NativeExtensionDraft] = field(default_factory=list)


class PlatformBackend(ABC):
    """Native-services contract used by the shared descriptor capture engine.

    The public observer protocol is intentionally tiny.  These backend hooks
    isolate pathname syntax, native identity, and platform timestamp behavior so
    the capture engine does not accidentally impose POSIX semantics on Windows.
    """

    platform_family: str

    @abstractmethod
    def assert_supported(self) -> None:
        """Raise when the backend cannot operate on the current host."""

    def absolute_path(self, path: PathInput) -> str | bytes:
        raw = os.fspath(path)
        if not isinstance(raw, (str, bytes)):
            raise TypeError("path must resolve to str or bytes")
        return os.path.abspath(raw)

    def preflight_path(self, path: str | bytes) -> None:
        result = os.lstat(path)
        if statmod.S_ISLNK(result.st_mode):
            raise SymlinkRefusedError("refusing to observe a symbolic-link final component")
        if not statmod.S_ISREG(result.st_mode):
            raise UnsupportedFileTypeError("target is not a regular file")

    def path_matches(self, path: str | bytes, stat: NativeStat) -> bool:
        try:
            result = os.lstat(path)
        except (FileNotFoundError, OSError):
            return False
        return statmod.S_ISREG(result.st_mode) and (result.st_dev, result.st_ino) == (
            stat.device,
            stat.inode,
        )

    def locator(
        self,
        path: str | bytes,
        *,
        kind: str,
        authority_id: str | None = None,
    ) -> JsonObject:
        return locator_from_path(path, kind=kind, authority_id=authority_id)

    def path_basename(self, path: str | bytes) -> str | bytes:
        return os.path.basename(path)

    def path_is_absolute(self, path: str | bytes) -> bool:
        return os.path.isabs(path)

    @abstractmethod
    def open_readonly(
        self, path: str | bytes, request: NativeCaptureRequest
    ) -> tuple[int, list[dict[str, object]], bool]:
        """Open without following the final symlink.

        Returns ``(fd, diagnostics, noatime_effective)``.
        """

    @abstractmethod
    def stat_fd(self, fd: int) -> NativeStat:
        """Read a source-native descriptor stat snapshot."""

    @abstractmethod
    def collect(
        self,
        fd: int,
        path: str | bytes,
        stat: NativeStat,
        request: NativeCaptureRequest,
    ) -> NativeCapture:
        """Capture platform-native metadata and the technical environment."""

    def finalize_timestamps(
        self,
        collection: NativeCapture,
        final_stat: NativeStat,
        request: NativeCaptureRequest,
    ) -> None:
        """Update observer-affected timestamps after all reads.

        POSIX backends store nanoseconds in NativeStat.  Windows overrides this
        hook to retain the source FILETIME tick value rather than reverse-
        converting through nanoseconds.
        """
        if not request.policy.include_access_time:
            return
        from time_formats import format_utc_ns

        for timestamp in collection.timestamps:
            if timestamp.get("kind") == "accessed":
                timestamp["value"] = format_utc_ns(final_stat.atime_ns)
                timestamp["raw_value"] = str(final_stat.atime_ns)

    def release_fd(self, fd: int) -> None:
        """Release backend bookkeeping immediately before the engine closes fd."""
        return None

    def stability_differences(self, before: NativeStat, after: NativeStat) -> list[str]:
        labels = (
            "device",
            "inode",
            "mode",
            "nlink",
            "uid",
            "gid",
            "size",
            "mtime_ns",
            "ctime_ns",
            "flags",
            "generation",
        )
        return [
            label
            for label, left, right in zip(
                labels, before.stability_key(), after.stability_key(), strict=True
            )
            if left != right
        ]


def format_provenance_count(value: int) -> str:
    """Encode a nonnegative journal count within its 63 bit domain."""

    return format_scalar("sequence63", value)


def utc_offset_string() -> str:
    now = dt.datetime.now().astimezone()
    offset = now.utcoffset()
    if offset is None:
        return "+00:00"
    total = int(offset.total_seconds())
    sign = "+" if total >= 0 else "-"
    total = abs(total)
    hours, remainder = divmod(total, 3600)
    minutes = remainder // 60
    return f"{sign}{hours:02d}:{minutes:02d}"


def safe_portable_text(value: str) -> str:
    if "\x00" in value or any(0xD800 <= ord(ch) <= 0xDFFF for ch in value):
        raise ValueError("text contains a JSON/PostgreSQL-incompatible character")
    return value


def path_bytes(path: str | bytes) -> bytes:
    return path if isinstance(path, bytes) else os.fsencode(path)


def bytes_display(data: bytes) -> str:
    return "bytes:" + urllib.parse.quote_from_bytes(data, safe="/-._~")


def portable_text_from_bytes(data: bytes) -> str:
    try:
        return safe_portable_text(data.decode("utf-8", "strict"))
    except (UnicodeDecodeError, ValueError):
        return bytes_display(data)


def locator_from_path(
    path: str | bytes,
    *,
    kind: str,
    authority_id: str | None = None,
) -> JsonObject:
    raw = path_bytes(path)
    try:
        text = raw.decode("utf-8", "strict")
        safe_portable_text(text)
        text_role = "exact"
        source_encoding = "UTF-8"
    except (UnicodeDecodeError, ValueError):
        text = bytes_display(raw)
        text_role = "display"
        source_encoding = None
    locator: JsonObject = {
        "syntax": "posix",
        "kind": kind,
        "text": text,
        "bytes": {
            "encoding": "base64",
            "data": base64.b64encode(raw).decode("ascii"),
            "byte_length": format_scalar("sequence63", len(raw)),
        },
        "text_role": text_role,
    }
    if source_encoding is not None:
        locator["source_encoding"] = source_encoding
    if authority_id is not None:
        locator["authority_id"] = authority_id
    return locator


def native_name_fields(name: bytes) -> JsonObject:
    encoded = {
        "encoding": "base64",
        "data": base64.b64encode(name).decode("ascii"),
        "byte_length": format_scalar("sequence63", len(name)),
    }
    try:
        text = safe_portable_text(name.decode("utf-8", "strict"))
    except (UnicodeDecodeError, ValueError):
        return {
            "name": bytes_display(name),
            "name_bytes": encoded,
            "name_role": "display",
        }
    return {
        "name": text,
        "name_bytes": encoded,
        "name_role": "exact",
        "name_source_encoding": "UTF-8",
    }


def source(platform: str, api: str, field: str | None = None) -> JsonObject:
    result: JsonObject = {"platform": platform, "api": api}
    if field is not None:
        result["field"] = field
    return result


def diagnostic(
    *,
    severity: str,
    category: str,
    code: str,
    message: str,
    native_code: str | None = None,
    source_descriptor: JsonObject | None = None,
) -> JsonObject:
    item: JsonObject = {
        "severity": severity,
        "category": category,
        "code": code,
        "message": message,
    }
    if native_code:
        item["native_code"] = native_code
    if source_descriptor:
        item["source"] = source_descriptor
    return item


def digest_assertion(
    value: str,
    *,
    agent_id: str,
    purpose: str = "fixity",
    algorithm: str = "sha-256",
) -> JsonObject:
    return {
        "algorithm": algorithm,
        "encoding": "hex",
        "value": value,
        "purpose": purpose,
        "originator_agent_id": agent_id,
    }


def bytes_value(data: bytes, *, agent_id: str | None = None) -> JsonObject:
    value: JsonObject = {
        "type": "bytes",
        "encoding": "base64",
        "data": base64.b64encode(data).decode("ascii"),
        "byte_length": format_scalar("sequence63", len(data)),
    }
    if agent_id is not None:
        value["digests"] = [
            digest_assertion(
                hashlib.sha256(data).hexdigest(),
                agent_id=agent_id,
                purpose="native_metadata",
            )
        ]
    return value


def digest_only_value(data: bytes, *, agent_id: str) -> JsonObject:
    return {
        "type": "digest",
        "byte_length": format_scalar("sequence63", len(data)),
        "digests": [
            digest_assertion(
                hashlib.sha256(data).hexdigest(),
                agent_id=agent_id,
                purpose="native_metadata",
            )
        ],
    }


def retained_native_value(
    data: bytes,
    *,
    agent_id: str,
    request: NativeCaptureRequest,
) -> tuple[str, JsonObject | None, str | None]:
    policy = request.policy
    if len(data) <= policy.inline_native_value_bytes:
        return "captured", bytes_value(data, agent_id=agent_id), None
    if len(data) > policy.maximum_native_value_bytes:
        if policy.large_value_disposition is LargeValueDisposition.FAIL:
            raise ValueError(f"native value length {len(data)} exceeds configured maximum")
        return (
            "not_retained",
            None,
            "Value exceeded maximum_native_value_bytes and was not retained.",
        )
    if policy.large_value_disposition is LargeValueDisposition.DIGEST_ONLY:
        return "digest_only", digest_only_value(data, agent_id=agent_id), None
    if policy.large_value_disposition is LargeValueDisposition.NOT_RETAINED:
        return (
            "not_retained",
            None,
            "Value exceeded inline_native_value_bytes and policy forbids retention.",
        )
    raise ValueError("native value exceeds inline threshold")


def timestamp_observation(
    *,
    kind: str,
    epoch_ns: int,
    platform: str,
    api: str,
    field: str,
    resolution_ns: int = 1,
) -> JsonObject:
    return {
        "kind": kind,
        "value_status": "exact",
        "value": format_utc_ns(epoch_ns),
        "resolution_ns": format_scalar("sequence63", max(1, resolution_ns)),
        "source": source(platform, api, field),
        "raw_value": str(epoch_ns),
        "raw_unit": "nanoseconds",
        "raw_epoch": "1970-01-01T00:00:00Z",
    }


def identifier(
    *,
    scheme: str,
    value: str,
    scope: str,
    authority_id: str | None = None,
) -> JsonObject:
    result: JsonObject = {
        "scheme": scheme,
        "value": value,
        "scope": scope,
        "representation": "clear",
    }
    if authority_id is not None:
        result["authority_id"] = authority_id
    return result


def observed_identifier(
    *,
    scheme: str,
    value: str,
    scope: str,
    platform: str,
    api: str,
    field: str | None = None,
    authority_id: str | None = None,
) -> JsonObject:
    result = identifier(scheme=scheme, value=value, scope=scope, authority_id=authority_id)
    result["source"] = source(platform, api, field)
    return result


def resolve_principal(
    *,
    numeric_id: int,
    kind: str,
    host_id: str,
    attempt_resolution: bool,
) -> JsonObject:
    scheme = "posix-uid" if kind == "user" else "posix-gid"
    principal: JsonObject = {
        "kind": kind,
        "identifiers": [
            identifier(
                scheme=scheme,
                value=str(numeric_id),
                scope="host",
                authority_id=host_id,
            )
        ],
        "resolution": "not_attempted",
    }
    if not attempt_resolution:
        return principal
    try:
        if kind == "user":
            import pwd

            name = pwd.getpwuid(numeric_id).pw_name
        else:
            import grp

            name = grp.getgrgid(numeric_id).gr_name
    except (KeyError, ImportError):
        principal["resolution"] = "unresolved"
    else:
        principal["name"] = safe_portable_text(name)
        principal["resolution"] = "resolved"
    return principal


def basic_access(stat: NativeStat, request: NativeCaptureRequest) -> JsonObject:
    return {
        "owner": resolve_principal(
            numeric_id=stat.uid,
            kind="user",
            host_id=request.host_id,
            attempt_resolution=request.policy.resolve_principals,
        ),
        "group": resolve_principal(
            numeric_id=stat.gid,
            kind="group",
            host_id=request.host_id,
            attempt_resolution=request.policy.resolve_principals,
        ),
        "posix_mode": f"{statmod.S_IMODE(stat.mode):04o}",
    }


def effective_principal(host_id: str) -> JsonObject:
    uid = os.geteuid() if hasattr(os, "geteuid") else 0
    return resolve_principal(
        numeric_id=uid,
        kind="user",
        host_id=host_id,
        attempt_resolution=True,
    )


def runtime_environment(host_id: str, *, include_principal: bool) -> JsonObject:
    runtime: JsonObject = {
        "process_architecture": os.uname().machine if hasattr(os, "uname") else "unknown",
        "time_zone": dt.datetime.now().astimezone().tzname() or "unknown",
        "utc_offset": utc_offset_string(),
        "character_encoding": locale.getpreferredencoding(False) or "unknown",
        "privilege": ("root" if hasattr(os, "geteuid") and os.geteuid() == 0 else "unprivileged"),
    }
    loc = locale.setlocale(locale.LC_CTYPE, None)
    if loc:
        runtime["locale"] = loc
    if include_principal and hasattr(os, "geteuid"):
        runtime["effective_principal"] = effective_principal(host_id)
    return runtime


def sparse_extents(
    fd: int, size: int, *, maximum_extents: int
) -> tuple[list[dict[str, int | str]], bool]:
    """Return SEEK_DATA/SEEK_HOLE extents and whether enumeration completed."""

    seek_data = getattr(os, "SEEK_DATA", 3)
    seek_hole = getattr(os, "SEEK_HOLE", 4)
    if size == 0:
        return [], True
    extents: list[dict[str, int | str]] = []
    position = 0
    complete = True
    while position < size:
        if len(extents) >= maximum_extents:
            complete = False
            break
        try:
            data_offset = os.lseek(fd, position, seek_data)
        except OSError as exc:
            if exc.errno in {6, 61}:  # ENXIO, platform variants
                if position < size:
                    extents.append({"kind": "hole", "offset": position, "length": size - position})
                break
            raise
        if data_offset > position:
            extents.append({"kind": "hole", "offset": position, "length": data_offset - position})
            if len(extents) >= maximum_extents:
                complete = False
                break
        try:
            hole_offset = os.lseek(fd, data_offset, seek_hole)
        except OSError as exc:
            if exc.errno in {6, 61}:  # no later hole
                hole_offset = size
            else:
                raise
        hole_offset = min(hole_offset, size)
        if hole_offset > data_offset:
            extents.append(
                {
                    "kind": "data",
                    "offset": data_offset,
                    "length": hole_offset - data_offset,
                }
            )
        if hole_offset <= position:
            complete = False
            break
        position = hole_offset
    os.lseek(fd, 0, os.SEEK_SET)
    return extents, complete


def make_sparse_map_row(
    extents: list[dict[str, int | str]],
    *,
    platform: str,
    agent_id: str,
    complete: bool,
) -> JsonObject:
    return {
        "kind": "sparse_map",
        "coverage_category": "storage_layout",
        "namespace": platform,
        "name": "data-and-hole-extents",
        "capture_status": "captured",
        "source": source(platform, "lseek(2)", "SEEK_DATA/SEEK_HOLE"),
        "value": {
            "type": "json",
            "schema": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/sparse-map.json",
            "data": {"complete": complete, "extents": extents},
        },
        "interpretations": [
            {
                "kind": "structured_parse",
                "schema": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/sparse-map.json",
                "value": {
                    "type": "json",
                    "schema": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/sparse-map.json",
                    "data": {"complete": complete, "extents": extents},
                },
                "agent_id": agent_id,
                "confidence": "high",
            }
        ],
        "sensitivity": "public",
    }


def merge_coverage(coverage: dict[str, str], category: str, status: str) -> None:
    """Merge independent capture attempts without hiding a partial failure."""

    priority = {
        "not_requested": 0,
        "not_applicable": 1,
        "not_supported": 2,
        "complete": 3,
        "partial": 4,
        "failed": 5,
    }
    current = coverage.get(category)
    if current is None or priority[status] > priority[current]:
        coverage[category] = status
