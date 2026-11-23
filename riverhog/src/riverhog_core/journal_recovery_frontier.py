"""Typed upload-journal continuation frozen inside an exact Native generation."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from riverhog_canonical_json import canonical_json_bytes, format_scalar, parse_scalar
from riverhog_provenance import JournalConstruction, JournalImage, JournalImageCapture

from riverhog_core.collection_recovery_state import RecoveryGrant, RecoveryRootBinding

if TYPE_CHECKING:
    from riverhog_core.services.upload_journal_validation_jobs import ValidationJob

JOURNAL_RECOVERY_DOMAIN = "upload-journal-validation"


def _owner(binding: RecoveryRootBinding, job: ValidationJob) -> dict[str, object]:
    if job.collection_id != binding.collection_id or job.journal_id is None:
        raise ValueError("journal recovery differs from its original construction")
    return {
        "construction": binding.document(),
        "workspace_id": job.workspace_id,
        "journal_id": job.journal_id,
        "attempt_id": job.attempt_id,
        "source_binding_json": job.source_binding_json,
        "certificate_binding_json": job.certificate_binding_json,
        "worker_epoch": format_scalar("nonnegative", job.worker_epoch),
    }


def journal_recovery_fence(binding: RecoveryRootBinding, job: ValidationJob) -> str:
    """Bind the grant to the original source and captured worker, never CAS version."""
    return hashlib.sha256(canonical_json_bytes(_owner(binding, job))).hexdigest()


@dataclass(frozen=True, slots=True)
class JournalRecoveryContext:
    binding: RecoveryRootBinding
    workspace_id: str
    journal_id: str
    attempt_id: str
    source_binding_json: str
    certificate_binding_json: str
    worker_epoch: int
    grant_identity: str
    encoded: bytes = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if (
            type(self.workspace_id) is not str
            or re.fullmatch(r"[0-9a-f]{32}", self.workspace_id) is None
            or type(self.binding.construction_sha256) is not str
            or re.fullmatch(r"[0-9a-f]{64}", self.binding.construction_sha256) is None
        ):
            raise ValueError("journal recovery requires its exact construction and workspace")
        for value in (
            self.journal_id,
            self.attempt_id,
            self.source_binding_json,
            self.certificate_binding_json,
        ):
            if type(value) is not str or not value:
                raise ValueError("journal recovery requires complete original source bindings")
        prefix = self.binding.grant_prefix
        if type(self.grant_identity) is not str or not self.grant_identity.startswith(prefix):
            raise ValueError("journal recovery grant belongs to another construction")
        if parse_scalar("nonnegative", self.grant_identity[len(prefix) :]) < 1:
            raise ValueError("journal recovery requires its issued positive grant identity")
        object.__setattr__(
            self,
            "encoded",
            canonical_json_bytes(
                {
                    "format": "riverhog-upload-journal-recovery/v1",
                    "phase": "validating",
                    "construction": self.binding.document(),
                    "workspace_id": self.workspace_id,
                    "journal_id": self.journal_id,
                    "attempt_id": self.attempt_id,
                    "source_binding_json": self.source_binding_json,
                    "certificate_binding_json": self.certificate_binding_json,
                    "worker_epoch": format_scalar("nonnegative", self.worker_epoch),
                    "grant_identity": self.grant_identity,
                }
            ),
        )

    @classmethod
    def original(
        cls, binding: RecoveryRootBinding, job: ValidationJob, grant: RecoveryGrant
    ) -> JournalRecoveryContext:
        _owner(binding, job)
        if (
            grant.domain != JOURNAL_RECOVERY_DOMAIN
            or grant.owner_id != job.workspace_id
            or grant.owner_fence != journal_recovery_fence(binding, job)
        ):
            raise ValueError("journal recovery grant differs from its original owner")
        assert job.journal_id is not None
        return cls(
            binding,
            job.workspace_id,
            job.journal_id,
            job.attempt_id,
            job.source_binding_json,
            job.certificate_binding_json,
            job.worker_epoch,
            grant.identity,
        )

    @classmethod
    def from_bytes(cls, raw: bytes) -> JournalRecoveryContext:
        try:
            value = json.loads(raw)
            if (
                type(value) is not dict
                or value.pop("format") != "riverhog-upload-journal-recovery/v1"
                or value.pop("phase") != "validating"
            ):
                raise ValueError("journal recovery context format differs")
            root = value.pop("construction")
            binding = RecoveryRootBinding(
                parse_scalar("sequence63", root["collection_id"]), root["construction_sha256"]
            )
            value["worker_epoch"] = parse_scalar("nonnegative", value["worker_epoch"])
            context = cls(binding=binding, **value)
            if context.encoded != raw:
                raise ValueError("journal recovery context is not canonical")
            return context
        except (TypeError, ValueError, KeyError, AttributeError, RecursionError) as error:
            raise ValueError("invalid original journal recovery context") from error


@dataclass(frozen=True, slots=True)
class JournalResumeFrontier:
    image: JournalImage
    context: JournalRecoveryContext = field(init=False)
    encoded: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        context = JournalRecoveryContext.from_bytes(self.image.owner_context)
        if (
            self.image.identity != context.grant_identity
            or self.image.attempt_id != context.workspace_id
            or self.image.worker_epoch != context.worker_epoch
        ):
            raise ValueError("Native image differs from its original Core continuation")
        object.__setattr__(self, "context", context)
        object.__setattr__(
            self,
            "encoded",
            canonical_json_bytes(
                {
                    "format": "riverhog-upload-journal-resume/v1",
                    "image": self.image.encoded.decode("utf-8"),
                }
            ).decode("utf-8"),
        )

    @property
    def generation(self) -> str:
        return self.image.envelope_sha256

    @classmethod
    def from_json(cls, raw: str, *, control_bytes: int) -> JournalResumeFrontier:
        if type(control_bytes) is not int or control_bytes < 1:
            raise ValueError("journal resume frontier requires positive metadata admission")
        if type(raw) is not str or len(raw.encode("utf-8")) > control_bytes:
            raise ValueError("journal resume frontier exceeds metadata admission")
        try:
            value = json.loads(raw)
            if (
                type(value) is not dict
                or set(value) != {"format", "image"}
                or value["format"] != "riverhog-upload-journal-resume/v1"
            ):
                raise ValueError("journal resume frontier format differs")
            frontier = cls(JournalImage.from_bytes(value["image"].encode("utf-8")))
            if frontier.encoded != raw:
                raise ValueError("journal resume frontier is not canonical")
            return frontier
        except (TypeError, ValueError, KeyError, AttributeError, RecursionError) as error:
            raise ValueError("invalid original journal resume frontier") from error


@dataclass(frozen=True, slots=True)
class JournalGenerationReceipt:
    """Immutable selected-original cost, after its actual Native receiving.

    This value proves no catalog publication. The surviving root and job must
    independently reconcile the same selected generation before accepting it.
    """

    context: JournalRecoveryContext
    generation: str
    total_actual_ns: int
    encoded: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if (
            type(self.generation) is not str
            or re.fullmatch(r"[0-9a-f]{64}", self.generation) is None
        ):
            raise ValueError("journal receiving requires its exact selected generation")
        object.__setattr__(
            self,
            "encoded",
            canonical_json_bytes(
                {
                    "format": "riverhog-upload-journal-generation-receipt/v1",
                    "context": self.context.encoded.decode("utf-8"),
                    "generation": self.generation,
                    "total_actual_ns": format_scalar("nonnegative", self.total_actual_ns),
                }
            ).decode("utf-8"),
        )

    @classmethod
    def from_original(
        cls,
        capture: JournalImageCapture,
        construction: JournalConstruction,
        *,
        total_actual_ns: int,
    ) -> JournalGenerationReceipt:
        frontier = JournalResumeFrontier(capture.received_image(construction))
        return cls(frontier.context, frontier.generation, total_actual_ns)

    @classmethod
    def from_json(cls, raw: str, *, control_bytes: int) -> JournalGenerationReceipt:
        if type(control_bytes) is not int or control_bytes < 1:
            raise ValueError("journal receiving requires positive metadata admission")
        if type(raw) is not str or len(raw.encode("utf-8")) > control_bytes:
            raise ValueError("journal generation receipt exceeds metadata admission")
        try:
            value = json.loads(raw)
            if (
                type(value) is not dict
                or set(value) != {"format", "context", "generation", "total_actual_ns"}
                or value["format"] != "riverhog-upload-journal-generation-receipt/v1"
            ):
                raise ValueError("journal generation receipt format differs")
            receipt = cls(
                JournalRecoveryContext.from_bytes(value["context"].encode("utf-8")),
                value["generation"],
                parse_scalar("nonnegative", value["total_actual_ns"]),
            )
            if receipt.encoded != raw:
                raise ValueError("journal generation receipt is not canonical")
            return receipt
        except (TypeError, ValueError, KeyError, AttributeError, RecursionError) as error:
            raise ValueError("invalid original journal generation receipt") from error
