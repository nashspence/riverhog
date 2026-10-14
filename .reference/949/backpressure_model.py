"""Non-authoritative sink-gated body-consumption witness; no Riverhog wire API.

Serial calls model authoritative receipt lookup and admission. The destination
consumes hashes/counts instead of storing an archive. There is no encryption,
network, real adapter, persistent payload, or physical durability implementation.
A source.read(n) MUST honor n; lower transport buffering is a separate allowance.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Callable, Protocol

from semantic_model import Deferred, LostReply, Rejected


@dataclass(frozen=True)
class Part:
    session: str
    number: int
    size: int
    sha256: str

    def validate(self, maximum_unit: int) -> None:
        if type(maximum_unit) is not int or not 1 <= maximum_unit <= 2**63 - 1:
            raise Rejected("invalid unit capacity")
        if not isinstance(self.session, str) or not 1 <= len(self.session.encode()) <= 128:
            raise Rejected("invalid bounded session identity")
        if len(json.dumps(self.session, ensure_ascii=False).encode()) > 258:
            raise Rejected("encoded session envelope exceeded")
        if type(self.number) is not int or not 1 <= self.number <= 2**63 - 1:
            raise Rejected("segment identity exhausted or invalid")
        if type(self.size) is not int or not 1 <= self.size <= maximum_unit:
            raise Rejected("unit exceeds bounded carrier; logical object may continue")
        if len(self.sha256) != 64 or any(c not in "0123456789abcdef" for c in self.sha256):
            raise Rejected("invalid digest")


class Source(Protocol):
    def read(self, maximum: int) -> bytes: ...


@dataclass
class Budget:
    """Shared transient allowance, reserved BEFORE body consumption, not after."""
    maximum_bytes: int
    maximum_calls: int
    used_bytes: int = 0
    calls: int = 0
    peak_bytes: int = 0

    def __post_init__(self) -> None:
        for value in (self.maximum_bytes, self.maximum_calls):
            if type(value) is not int or value < 1:
                raise Rejected("invalid capacity")

    def reserve(self, size: int) -> None:
        if type(size) is not int or size < 1:
            raise Rejected("invalid reservation")
        if size > self.maximum_bytes - self.used_bytes or self.calls >= self.maximum_calls:
            raise Deferred("transient capacity exhausted")
        self.used_bytes += size
        self.calls += 1
        self.peak_bytes = max(self.peak_bytes, self.used_bytes)

    def release(self, size: int) -> None:
        if not 0 < size <= self.used_bytes or self.calls < 1:
            raise Rejected("reservation accounting underflow")
        self.used_bytes -= size
        self.calls -= 1


class Destination:
    """One ordered fixture session; last receipt survives response loss/offline.

    Prior accepted archive content belongs to the destination. This compact model
    does not replace Riverhog's exact segment traversal or completed-object fences.
    """
    def __init__(self, session: str = "s") -> None:
        self.session = session
        self.ready = False
        self.last: Part | None = None
        self.committed_bytes = 0
        self.lookups = 0
        self.admissions = 0
        self.effects = 0
        self.attempted_bytes = 0
        self.lose_reply = False
        self.after_chunk: Callable[[], None] = lambda: None

    def lookup(self, part: Part) -> bool:
        self.lookups += 1
        if part.session != self.session:
            raise Rejected("different session")
        if self.last is not None and part.number == self.last.number:
            if part != self.last:
                raise Rejected("changed exact request")
            return True
        expected = 1 if self.last is None else self.last.number + 1
        if part.number != expected:
            raise Rejected("noncontiguous or compacted historical request")
        return False

    def admit(self) -> None:
        self.admissions += 1
        if not self.ready:
            raise Deferred("destination cannot accept this body")

    def accept(self, part: Part, source: Source, chunk_bytes: int) -> None:
        self.effects += 1
        digest = hashlib.sha256()
        remaining = part.size
        while remaining:
            if not self.ready:
                raise Deferred("availability lost at effect boundary")
            data = source.read(min(chunk_bytes, remaining))
            if not isinstance(data, bytes) or not 0 < len(data) <= min(chunk_bytes, remaining):
                raise Rejected("source violated bounded exact framing")
            digest.update(data)
            remaining -= len(data)
            self.attempted_bytes += len(data)
            self.after_chunk()
        if not self.ready:
            raise Deferred("availability lost before receipt")
        if digest.hexdigest() != part.sha256:
            raise Rejected("digest conflict")
        self.last = part
        self.committed_bytes += part.size
        if self.lose_reply:
            self.lose_reply = False
            raise LostReply("effect committed but receipt was lost")


class Upload:
    """One pending exact part, with metadata-only restart state.

    Instantiation represents prior durable/control-capacity admission (see the
    separate work-budget witness). A waiting request does not own a request body.
    A real paused worker MUST be fenced before dropping its resource reservation;
    this serial model only releases reservations after synchronous calls return.
    """
    def __init__(self, part: Part, *, maximum_unit: int = 4096, chunk_bytes: int = 64):
        part.validate(maximum_unit)
        if type(chunk_bytes) is not int or not 1 <= chunk_bytes <= maximum_unit:
            raise Rejected("invalid receive bound")
        self.part = part
        self.maximum_unit = maximum_unit
        self.chunk_bytes = chunk_bytes
        self.phase = "pending"
        self.body_reads = 0
        self.bytes_read = 0

    def snapshot(self) -> dict[str, object]:
        # Diagnostic counters are deliberately not an append-only durable history.
        return {"part": asdict(self.part), "phase": self.phase,
                "maximum_unit": self.maximum_unit, "chunk_bytes": self.chunk_bytes}

    @classmethod
    def restore(cls, state: dict) -> Upload:
        result = cls(Part(**state["part"]), maximum_unit=state["maximum_unit"],
                     chunk_bytes=state["chunk_bytes"])
        if state["phase"] not in {"pending", "waiting", "reconcile", "accepted", "failed"}:
            raise Rejected("unknown checkpoint phase")
        result.phase = state["phase"]
        return result

    def attempt(self, destination: Destination, budget: Budget,
                source_factory: Callable[[], Source], *,
                after_admission: Callable[[], None] = lambda: None) -> str:
        if self.phase == "accepted":
            return self.phase
        if self.phase == "failed":
            raise Rejected("definite failure needs caller disposition")
        # Reserve old + newly received chunk. Production reserves simultaneous
        # body, crypto, copies, framing and transport memory, not just this number.
        reserved = False
        try:
            if destination.lookup(self.part):
                self.phase = "accepted"
                return self.phase  # No retransmission even if currently offline.
            budget.reserve(2 * self.chunk_bytes)
            reserved = True
            destination.admit()
            after_admission()
            # Recheck at the effect boundary; a preflight is not a guarantee.
            if not destination.ready:
                raise Deferred("admission became stale before source consumption")
            owner = self
            source = source_factory()

            class CountedSource:
                def read(self, maximum: int) -> bytes:
                    data = source.read(maximum)
                    owner.body_reads += 1
                    owner.bytes_read += len(data)
                    return data

            destination.accept(self.part, CountedSource(), self.chunk_bytes)
            self.phase = "accepted"
        except Deferred:
            self.phase = "waiting"
        except (LostReply, TimeoutError):
            self.phase = "reconcile"
        except Exception:
            self.phase = "failed"
            raise
        finally:
            if reserved:
                budget.release(2 * self.chunk_bytes)
        return self.phase
