"""Owner-pinned private rank evidence for complete finite journal replay.

Certificates cover only graph acyclicity at the native edge check. Ordered
schema, identity, reference, correction, profile and terminal checks retain
their native authority. A cyclic or deferred epoch uses the online checker.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Protocol

from riverhog_canonical_json import canonical_json_bytes
from riverhog_provenance_contracts import require_canonical_uuid_urn

from .replay_binding import JournalReplayBinding

JOURNAL_GRAPH_CERTIFICATE_ALGORITHM = "riverhog-journal-graph-certificate/v1"


class InvalidJournalGraphCertificate(RuntimeError):
    """Retained execution evidence is missing, inconsistent or bound elsewhere."""


def _digest(value: object) -> None:
    if type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise InvalidJournalGraphCertificate("graph certificate requires an exact digest")


@dataclass(frozen=True, slots=True)
class JournalGraphCertificateBinding:
    source: JournalReplayBinding
    manifest_sha256: str

    def __post_init__(self) -> None:
        if type(self.source) is not JournalReplayBinding:
            raise InvalidJournalGraphCertificate(
                "graph certificate requires its exact finite source"
            )
        _digest(self.manifest_sha256)

    def to_json(self) -> str:
        return canonical_json_bytes(
            {
                "algorithm": JOURNAL_GRAPH_CERTIFICATE_ALGORITHM,
                "source": self.source.to_json(),
                "manifest_sha256": self.manifest_sha256,
            }
        ).decode()

    def checkpoint_sha256(self) -> str:
        return hashlib.sha256(self.to_json().encode()).hexdigest()

    @classmethod
    def from_json(cls, raw: str) -> JournalGraphCertificateBinding:
        """Restore only the exact canonical owner-pinned certificate binding."""
        try:
            value = json.loads(raw)
            if (
                type(value) is not dict
                or set(value) != {"algorithm", "source", "manifest_sha256"}
                or value["algorithm"] != JOURNAL_GRAPH_CERTIFICATE_ALGORITHM
            ):
                raise ValueError("invalid graph certificate binding protocol")
            binding = cls(JournalReplayBinding.from_json(value["source"]), value["manifest_sha256"])
            if binding.to_json() != raw:
                raise ValueError("noncanonical graph certificate binding")
            return binding
        except (RuntimeError, TypeError, ValueError, KeyError) as error:
            raise InvalidJournalGraphCertificate(
                "invalid retained graph certificate binding"
            ) from error


@dataclass(frozen=True, slots=True)
class JournalGraphCertificate:
    binding_sha256: str
    frame_ordinal: int
    frame_sha256: str
    epoch: int
    kind: str
    source: str
    target: str
    source_rank: int
    target_rank: int
    vertex_count: int
    arc_count: int

    def verify(
        self,
        binding: JournalGraphCertificateBinding,
        *,
        frame_ordinal: int,
        frame_sha256: str,
        kind: str,
        source: str,
        target: str,
    ) -> None:
        try:
            _digest(self.binding_sha256)
            _digest(self.frame_sha256)
            for identity in (self.source, self.target):
                require_canonical_uuid_urn(identity)
            if self.kind not in {"derivation", "specialization"}:
                raise ValueError("graph kind")
            for quantity in (
                self.frame_ordinal,
                self.epoch,
                self.source_rank,
                self.target_rank,
                self.vertex_count,
                self.arc_count,
            ):
                if type(quantity) is not int or quantity < 0:
                    raise ValueError("graph certificate scalar")
            if not (
                self.epoch <= self.frame_ordinal < int(binding.source.frame_count)
                and self.source_rank < self.target_rank < self.vertex_count
                and self.arc_count > 0
            ):
                raise ValueError("graph certificate ranks or extent")
            if (
                self.binding_sha256 != binding.checkpoint_sha256()
                or self.frame_ordinal != frame_ordinal
                or self.frame_sha256 != frame_sha256
                or (self.kind, self.source, self.target) != (kind, source, target)
            ):
                raise ValueError("graph certificate semantic input")
        except (ValueError, TypeError) as exc:
            raise InvalidJournalGraphCertificate(
                "graph certificate differs from its exact semantic input or completed rank"
            ) from exc

    def encoded_octets(self) -> int:
        return len(
            canonical_json_bytes({name: str(getattr(self, name)) for name in self.__slots__})
        )


class JournalGraphCertificates(Protocol):
    """Bounded closed-resource reads from an owner-created completed rank manifest.

    The immutable binding names the exact retained finite source and selected
    epoch/rank algorithms and completed headers. For an acyclic epoch/kind,
    every requested native edge must exist in that union and have both point
    ranks. Missing evidence fails explicitly. Return None only for an entire
    cyclic or deferred epoch/kind, consistently across calls and restarts.
    Neither imported client tables nor an unverified rank claim satisfy this
    port. Reads return detached metadata and never retain catalog resources.
    """

    @property
    def binding(self) -> JournalGraphCertificateBinding: ...

    def edge(
        self,
        *,
        frame_ordinal: int,
        frame_sha256: str,
        kind: str,
        source: str,
        target: str,
        byte_budget: int,
    ) -> JournalGraphCertificate | None: ...
