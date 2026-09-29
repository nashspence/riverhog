"""Descriptor-bound native file acquisition for the canonical observer port."""

from __future__ import annotations

import base64
import os
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import Any

from riverhog_provenance_contracts import (
    ContractCatalog,
    ProvenanceContractBinding,
    canonical_document,
    require_canonical_uuid_urn,
)
from riverhog_provenance_contracts.codec import require_portable_json

from .common import assertion, evidence, new_id, reference, utc_now
from .constants import PROFILE, PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
from .errors import ObservationError
from .interface import ObservationSource
from .model import (
    BinaryReadable,
    ObservationRequest,
    ObservationResult,
    ObservationSession,
    SourceCapabilities,
    SourceEvidence,
)
from .native_capture import (
    NativeCapture,
    NativeCapturePolicy,
    NativeCaptureRequest,
    PathInput,
    PlatformBackend,
    UnstableFileError,
)
from .observer import BoundedSourceObserver

SOURCE_NAMING_VIEW_SCHEME = (
    "https://nashspence.github.io/riverhog/v1/provenance/identifiers/source-naming-view"
)


def _portable_native(value: Any) -> Any:
    """Retain exact wide numbers as tagged decimals within portable profile JSON."""

    if isinstance(value, Mapping):
        return {str(key): _portable_native(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_portable_native(item) for item in value]
    if type(value) is int and abs(value) > (1 << 53) - 1:
        return {"kind": "integer-decimal", "value": str(value)}
    return value


def filesystem_name(path: str | bytes, platform: str) -> dict[str, Any]:
    if platform == "windows":
        if not isinstance(path, str):
            raise ObservationError("Windows native pathname must be Unicode")
        raw = path.encode("utf-16le", "surrogatepass")
        encoding = "utf-16le"
    else:
        raw = os.fsencode(path)
        try:
            raw.decode("utf-8", "strict")
        except UnicodeDecodeError:
            encoding = "posix-bytes"
        else:
            encoding = "utf-8"
    return {
        "kind": "bytes",
        "bytes": {
            "encoding": "base64",
            "data": base64.b64encode(raw).decode("ascii"),
            "byte_length": str(len(raw)),
        },
        "encoding": encoding,
    }


class _DescriptorReader:
    def __init__(self, fd: int) -> None:
        self.fd = fd

    def read(self, size: int = -1, /) -> bytes:
        if size < 0:
            raise ObservationError("native source requires bounded reads")
        return os.read(self.fd, size)


class NativeFileSource:
    """Open one real regular file without turning its path into member identity."""

    def __init__(
        self,
        path: PathInput,
        *,
        backend: PlatformBackend,
        contract: ProvenanceContractBinding,
        native_schema_id: str,
        host_id: str,
        naming_view_id: str | None = None,
        policy: NativeCapturePolicy | None = None,
    ) -> None:
        self.path = path
        self.backend = backend
        self.contract = contract
        self.native_schema_id = native_schema_id
        self.host_id = require_canonical_uuid_urn(host_id, "native host authority")
        self.naming_view_id = (
            require_canonical_uuid_urn(naming_view_id, "source naming view")
            if naming_view_id is not None
            else None
        )
        self.policy = policy or NativeCapturePolicy()
        if native_schema_id not in contract.schemas:
            raise ValueError("native capture schema is absent from its pinned contract")

    @contextmanager
    def open(self, *, observer_agent_id: str) -> Iterator[ObservationSession]:
        backend = self.backend
        backend.assert_supported()
        absolute = backend.absolute_path(self.path)
        backend.preflight_path(absolute)
        request = NativeCaptureRequest(
            host_id=self.host_id, observer_agent_id=observer_agent_id, policy=self.policy
        )
        context_id = new_id()
        context_fields: dict[str, Any] = {"kind": "filesystem_namespace"}
        if self.naming_view_id is not None:
            context_fields["identifiers"] = [
                {
                    "scheme": SOURCE_NAMING_VIEW_SCHEME,
                    "value": {"kind": "text", "text": self.naming_view_id},
                    "scope": "global",
                }
            ]
        source_context = assertion(
            "context", observer_agent_id, object_id=context_id, **context_fields
        )
        fd = -1
        try:
            fd, open_diagnostics, noatime_effective = backend.open_readonly(absolute, request)
            before = backend.stat_fd(fd)
            if not backend.path_matches(absolute, before):
                raise UnstableFileError("source path changed between preflight and descriptor open")
            opened_at = utc_now()

            def finalize() -> SourceEvidence:
                post_read = backend.stat_fd(fd)
                if post_read.size != before.size:
                    raise UnstableFileError("native source size changed during measurement")
                collection = backend.collect(fd, absolute, post_read, request)
                after = backend.stat_fd(fd)
                backend.finalize_timestamps(collection, after, request)
                differences = backend.stability_differences(before, after)
                if self.policy.verify_path_binding and not backend.path_matches(absolute, after):
                    differences.append("path_identity")
                if differences and self.policy.strict_consistency:
                    raise UnstableFileError(
                        "native source changed during strict observation: " + ", ".join(differences)
                    )
                native_data = self._native_profile_data(
                    collection,
                    open_diagnostics=open_diagnostics,
                    noatime_effective=noatime_effective,
                )
                require_portable_json(native_data)
                if (
                    len(canonical_document(native_data))
                    > PROVENANCE_JOURNAL_ENTRY_BYTES_MAX - 2 * 1024 * 1024
                ):
                    raise ObservationError("native capture profile exceeds bounded journal budget")
                profile = {
                    "profile": {
                        "contract_id": self.contract.contract_id,
                        "contract_sha256": self.contract.contract_sha256,
                        "schema_id": self.native_schema_id,
                    },
                    "data": native_data,
                }
                extensions = self._native_extensions(
                    collection,
                    context_id=context_id,
                    observer_agent_id=observer_agent_id,
                )
                coverage = tuple(
                    {
                        "profile_id": PROFILE + "/observers/" + backend.platform_family,
                        "category": PROFILE + "/observers/coverage/" + category,
                        "status": "not_exposed" if status == "not_supported" else status,
                        **(
                            {}
                            if status == "complete"
                            else {"reason": f"Native {category} capture reported {status}."}
                        ),
                    }
                    for category, status in sorted(collection.coverage.items())
                )
                consistency: dict[str, Any]
                if differences:
                    consistency = {
                        "level": "best_effort",
                        "method_uri": PROFILE + "/methods/native-descriptor-observation",
                        "limitations": [
                            "Native source stability changed: " + ", ".join(differences)
                        ],
                    }
                else:
                    consistency = {
                        "level": "verified_unchanged",
                        "method_uri": PROFILE + "/methods/native-descriptor-observation",
                        "checks": ["descriptor stat before and after byte measurement"],
                    }
                locator = {
                    "context_id": context_id,
                    "locator": {
                        "kind": "filesystem_path",
                        "syntax": "windows" if backend.platform_family == "windows" else "posix",
                        "form": "absolute",
                        "name": filesystem_name(absolute, backend.platform_family),
                    },
                    "temporal_scope": {"kind": "instant", "at": opened_at},
                }
                return SourceEvidence(
                    consistency=consistency,
                    address_status="known",
                    profiles=(profile,),
                    coverage=coverage,
                    locators=(locator,),
                    supporting_assertions={"extensions": extensions} if extensions else {},
                )

            @contextmanager
            def repeat_reader() -> Iterator[BinaryReadable]:
                os.lseek(fd, 0, os.SEEK_SET)
                yield _DescriptorReader(fd)

            yield ObservationSession(
                reader=_DescriptorReader(fd),
                extent={"kind": "whole_object"},
                occurrence_kind="filesystem_object",
                capabilities=SourceCapabilities(
                    repeatable=True,
                    seekable=True,
                    native_metadata=True,
                    persistent_designator=True,
                ),
                expected_length=before.size,
                source_context=source_context,
                finalize=finalize,
                repeat_reader=repeat_reader,
            )
        finally:
            if fd >= 0:
                backend.release_fd(fd)
                os.close(fd)

    def _native_profile_data(
        self,
        collection: NativeCapture,
        *,
        open_diagnostics: list[dict[str, object]],
        noatime_effective: bool,
    ) -> dict[str, Any]:
        environment = dict(collection.environment or {})
        if environment.get("id") == "urn:uuid:00000000-0000-0000-0000-000000000000":
            del environment["id"]
        return _portable_native(
            {
                "object_kind": "regular_file",
                "environment": environment,
                "timestamps": collection.timestamps,
                "access": collection.access,
                "native_identifiers": collection.native_identifiers,
                "native_metadata": collection.native_metadata,
                "coverage": collection.coverage,
                "diagnostics": [*open_diagnostics, *collection.diagnostics],
                "noatime_effective": noatime_effective,
            }
        )

    def _native_extensions(
        self,
        collection: NativeCapture,
        *,
        context_id: str,
        observer_agent_id: str,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for draft in collection.extension_drafts:
            value = draft.value
            if (
                draft.subject_role != "environment"
                or value.get("type") != "json"
                or not isinstance(value.get("schema"), str)
                or not isinstance(value.get("data"), dict)
                or value["schema"] not in self.contract.schemas
            ):
                raise ObservationError("native extension is not an exact pinned context profile")
            rows.append(
                assertion(
                    "extension",
                    observer_agent_id,
                    subject=reference(context_id, "context"),
                    property=draft.property,
                    value={
                        "type": "json",
                        "value": {
                            "profile": {
                                "contract_id": self.contract.contract_id,
                                "contract_sha256": self.contract.contract_sha256,
                                "schema_id": value["schema"],
                            },
                            "data": _portable_native(value["data"]),
                        },
                    },
                    evidence_items=[
                        evidence(
                            observer_agent_id,
                            "direct_measurement",
                            method_uri=PROFILE + "/methods/native-descriptor-observation",
                        )
                    ],
                )
            )
        return rows


class NativeFileObserver:
    """Canonical observer for native sources of one pinned platform contract."""

    def __init__(self, contract: ProvenanceContractBinding, platform_family: str) -> None:
        self._contract = contract
        self.platform_family = platform_family
        self._observer = BoundedSourceObserver(catalog=ContractCatalog((contract,)))

    @property
    def contract_binding(self) -> ProvenanceContractBinding:
        return self._contract

    def observe(
        self, source: ObservationSource, request: ObservationRequest | None = None
    ) -> ObservationResult:
        if not isinstance(source, NativeFileSource):
            raise TypeError("native observer requires a NativeFileSource")
        if (
            source.backend.platform_family != self.platform_family
            or source.contract.contract_sha256 != self._contract.contract_sha256
        ):
            raise ValueError("native source and observer contract disagree")
        return self._observer.observe(source, request)


__all__ = ["NativeFileObserver", "NativeFileSource"]
