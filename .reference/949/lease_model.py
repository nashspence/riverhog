"""Non-authoritative #949 finite-ownership witness, not a proposed wire API.

Transitions are serial/atomic; snapshots assume committed durable state. Namespaces
stand for already authenticated caller scopes. Their selection is NOT authorization.
The sliding retirement window is ONE constructive bounded-fence design, not a v1
requirement. No devices, real clocks, databases, worker fencing, or I/O are implemented.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any

from semantic_model import Deferred, LostReply, Rejected


class Retired(Rejected):
    """Old ownership cannot be recreated; this is not a historical effect receipt."""


@dataclass(frozen=True)
class Consumption:
    incarnation: str
    namespace: str
    ordinal: int
    object_key: str  # Exact object AND revision, symbolically represented.


@dataclass(frozen=True)
class LeaseView:
    phase: str
    revision: int | None
    expires_at: int | None


class LeaseAdapterModel:
    """Finite grants plus bounded per-scope retirement windows and stream pins.

    A persisted retired-through watermark rejects arbitrarily late old messages.
    Records above it occupy a fixed window. An unresolved low ordinal can cause
    backpressure, never unsafe history deletion. The watermark is not reset on
    restart, caller key rotation, or registration-name reuse.
    """

    def __init__(
        self, incarnation: str, routes: dict[str, str], namespaces: tuple[str, ...],
        *, window: int = 8, maximum_lease: int = 30, maximum_streams: int = 2,
        maximum_ordinal: int = 2**63 - 1,
    ) -> None:
        for value in (window, maximum_lease, maximum_streams, maximum_ordinal):
            if type(value) is not int or value <= 0:
                raise Rejected("bounds must be positive integers")
        if not namespaces or len(set(namespaces)) != len(namespaces):
            raise Rejected("finite unique caller scopes are required")
        self.state: dict[str, Any] = {
            "incarnation": incarnation, "routes": dict(routes), "now": 0,
            "window": window, "maximum_lease": maximum_lease,
            "maximum_streams": maximum_streams, "maximum_ordinal": maximum_ordinal,
            "scopes": {n: {"retired_through": 0, "records": {}} for n in namespaces},
        }
        self.online: set[str] = set()

    def snapshot(self) -> str:
        return json.dumps(self.state, sort_keys=True)

    @classmethod
    def restore(cls, snapshot: str) -> LeaseAdapterModel:
        state = json.loads(snapshot)
        result = cls(state["incarnation"], state["routes"], tuple(state["scopes"]))
        result.state = state
        # Neither availability nor real worker cessation follows from a restart.
        return result

    def advance_to(self, now: int) -> None:
        if type(now) is not int or now < self.state["now"]:
            raise Rejected("clock regression or invalid clock; fail closed")
        self.state["now"] = now

    def _scope(self, identity: Consumption) -> dict[str, Any]:
        if identity.incarnation != self.state["incarnation"]:
            raise Rejected("incarnation changed")
        if identity.namespace not in self.state["scopes"]:
            raise Rejected("unknown caller scope")
        if identity.object_key not in self.state["routes"]:
            raise Rejected("unknown exact object")
        if type(identity.ordinal) is not int or not 1 <= identity.ordinal <= self.state["maximum_ordinal"]:
            raise Rejected("ordinal exhausted or invalid; never wrap/reuse")
        return self.state["scopes"][identity.namespace]

    def _record(self, identity: Consumption) -> dict[str, Any] | None:
        scope = self._scope(identity)
        if identity.ordinal <= scope["retired_through"]:
            raise Retired("ownership retired; exact historical response was compacted")
        record = scope["records"].get(str(identity.ordinal))
        if record is not None and record["identity"] != asdict(identity):
            raise Rejected("consumer reused for another exact object/revision")
        if record is not None:
            self._expire(record)
        return record

    def _expire(self, record: dict[str, Any]) -> None:
        if record["phase"] == "active" and self.state["now"] >= record["expires_at"]:
            record["phase"] = "expired"
            # Expiry fences new admission/renewal, not existing streams/effects.

    def _insert(self, identity: Consumption, seconds: int | None) -> dict[str, Any]:
        scope = self._scope(identity)
        if identity.ordinal > scope["retired_through"] + self.state["window"]:
            raise Deferred("bounded ownership/replay window is full or has unresolved gaps")
        record = {
            "identity": asdict(identity), "initial_seconds": seconds,
            "phase": "active" if seconds is not None else "released",
            "revision": 0, "expires_at": self.state["now"] + (seconds or 0),
            "last_renewal": None, "last_stream": 0, "pins": [],
            "cleanup_required": False,
        }
        scope["records"][str(identity.ordinal)] = record
        return record

    @staticmethod
    def _view(record: dict[str, Any]) -> LeaseView:
        return LeaseView(record["phase"], record["revision"], record["expires_at"])

    def prepare(self, identity: Consumption, seconds: int) -> LeaseView:
        if type(seconds) is not int or not 1 <= seconds <= self.state["maximum_lease"]:
            raise Rejected("lease must be finite and within the admission bound")
        record = self._record(identity)
        if record is None:
            record = self._insert(identity, seconds)
        elif record["phase"] != "active":
            raise Retired("prepare cannot recreate expired/released ownership")
        elif record["initial_seconds"] != seconds:
            raise Rejected("prepare replay changed its immutable request")
        return self._view(record)  # A replay does NOT refresh expiry.

    def status(self, identity: Consumption) -> LeaseView:
        try:
            record = self._record(identity)
        except Retired:
            return LeaseView("retired", None, None)
        if record is None:
            return LeaseView("unknown", None, None)  # Not release evidence.
        return self._view(record)

    def renew(self, identity: Consumption, expected_revision: int, until: int) -> LeaseView:
        record = self._record(identity)
        if record is None or record["phase"] != "active":
            raise Retired("renewal never creates or resurrects ownership")
        if type(expected_revision) is not int or expected_revision < 0 or type(until) is not int:
            raise Rejected("invalid renewal identity")
        request = [expected_revision, until]
        if record["last_renewal"] == request:
            return self._view(record)  # Exact last retry, not now + duration again.
        if record["revision"] != expected_revision:
            raise Rejected("stale renewal; reconcile current lease")
        if not record["expires_at"] < until <= self.state["now"] + self.state["maximum_lease"]:
            raise Rejected("renewal must extend the lease within its finite bound")
        record["revision"] += 1
        record["expires_at"] = until
        record["last_renewal"] = request  # O(1), not a history of renewals.
        return self._view(record)

    def open_read(self, identity: Consumption, stream: int) -> None:
        record = self._record(identity)
        if record is None or record["phase"] != "active":
            raise Retired("ownership cannot admit a new stream")
        if type(stream) is not int or not 1 <= stream <= self.state["maximum_ordinal"]:
            raise Rejected("invalid stream ordinal")
        if stream in record["pins"]:
            raise Rejected("stream already admitted; reconcile, do not start duplicate I/O")
        if stream <= record["last_stream"]:
            raise Retired("closed/delayed stream admission cannot be replayed")
        if len(record["pins"]) >= self.state["maximum_streams"]:
            raise Deferred("bounded active stream capacity")
        if self.state["routes"][identity.object_key] not in self.online:
            raise Deferred("resource not currently available")
        record["last_stream"] = stream
        record["pins"].append(stream)

    def confirm_stream_closed(self, identity: Consumption, stream: int) -> None:
        """Adapter-side evidence that the exact stream/worker can no longer do I/O.

        A caller timeout, expired lease or process restart is NOT such evidence.
        Real worker fencing/drain is assumed by this model transition, not proved.
        """
        try:
            record = self._record(identity)
        except Retired:
            return
        if record is None:
            raise Rejected("unknown stream owner")
        if stream in record["pins"]:
            record["pins"].remove(stream)

    def record_cleanup_effect(self, identity: Consumption) -> None:
        """Fixture: preparation committed a provider effect requiring later cleanup."""
        record = self._record(identity)
        if record is None or record["phase"] != "active":
            raise Rejected("no active preparation")
        record["cleanup_required"] = True

    def perform_cleanup(self, identity: Consumption) -> None:
        record = self._record(identity)
        if record is None or record["phase"] == "active":
            raise Rejected("ownership still active or unknown")
        if record["pins"]:
            raise Deferred("streams are still pinned")
        if record["cleanup_required"]:
            if self.state["routes"][identity.object_key] not in self.online:
                raise Deferred("required cleanup effect is not complete")
            record["cleanup_required"] = False  # Abstract external completion point.

    @staticmethod
    def _settled(record: dict[str, Any]) -> bool:
        return record["phase"] != "active" and not record["pins"] and not record["cleanup_required"]

    def release(self, identity: Consumption) -> None:
        try:
            record = self._record(identity)
        except Retired:
            return  # Watermark proves ownership settled, NOT object deletion/history.
        if record is None:
            record = self._insert(identity, None)  # Fence release-before-prepare.
        record["phase"] = "released"
        if not self._settled(record):
            raise Deferred("admission closed but release effects remain unsettled")

    def collect(self, namespace: str, *, limit: int) -> int:
        """Compact only a settled contiguous prefix; at most limit records retired."""
        if namespace not in self.state["scopes"] or type(limit) is not int or limit < 1:
            raise Rejected("invalid bounded collection request")
        scope = self.state["scopes"][namespace]
        count = 0
        while count < min(limit, self.state["window"]):
            ordinal = scope["retired_through"] + 1
            record = scope["records"].get(str(ordinal))
            if record is None:
                break  # Never infer that an unobserved prepare cannot still arrive.
            self._expire(record)
            if not self._settled(record):
                break
            del scope["records"][str(ordinal)]
            scope["retired_through"] = ordinal
            count += 1
        return count

    def busy(self, resource: str) -> bool:
        for scope in self.state["scopes"].values():
            for record in scope["records"].values():
                self._expire(record)
                if self.state["routes"][record["identity"]["object_key"]] == resource and not self._settled(record):
                    return True
        return False

    def quiesce(self, resource: str) -> bool:
        """Atomic model admission gate closure; never an OS safe-to-eject promise."""
        if resource not in self.state["routes"].values():
            raise Rejected("unknown resource")
        if self.busy(resource):
            return False
        self.online.discard(resource)
        return True


class LeaseConsumerModel:
    """A durably allocated claim/cleanup intent, persisted before any external call.

    Production must allocate ordinals without reuse within an authenticated scope,
    retain allocation-gap cleanup, and prevent state rollback. That allocator is
    assumed, not implemented by this single-claim snapshot witness.
    """

    def __init__(self, identity: Consumption, seconds: int) -> None:
        self.state: dict[str, Any] = {
            "identity": asdict(identity), "seconds": seconds, "outcome": "pending",
            "cleanup_pending": True, "lease": None, "pending_renewal": None,
        }

    @property
    def identity(self) -> Consumption:
        return Consumption(**self.state["identity"])

    def snapshot(self) -> str:
        return json.dumps(self.state, sort_keys=True)

    @classmethod
    def restore(cls, snapshot: str) -> LeaseConsumerModel:
        state = json.loads(snapshot)
        result = cls(Consumption(**state["identity"]), state["seconds"])
        result.state = state
        return result

    def prepare(self, adapter: LeaseAdapterModel, *, lose_reply: bool = False) -> None:
        if self.state["outcome"] != "pending":
            raise Rejected("consumer terminal")
        grant = adapter.prepare(self.identity, self.state["seconds"])
        if lose_reply:
            raise LostReply("prepare response lost; cleanup obligation already persisted")
        self.state["lease"] = asdict(grant)

    def renew(self, adapter: LeaseAdapterModel, until: int, *, lose_reply: bool = False) -> None:
        if self.state["outcome"] != "pending" or self.state["lease"] is None:
            raise Rejected("no live caller grant")
        request = self.state["pending_renewal"]
        if request is None:
            request = [self.state["lease"]["revision"], until]
            self.state["pending_renewal"] = request  # Persist before dispatch.
        elif request[1] != until:
            raise Rejected("reconcile pending renewal before changing its request")
        grant = adapter.renew(self.identity, request[0], request[1])
        if lose_reply:
            raise LostReply("renewal response lost")
        self.state["lease"] = asdict(grant)
        self.state["pending_renewal"] = None

    def settle(self, outcome: str) -> None:
        if outcome not in {"succeeded", "failed", "canceled", "expired"}:
            raise Rejected("invalid terminal outcome")
        if self.state["outcome"] not in {"pending", outcome}:
            raise Rejected("cannot change terminal outcome")
        self.state["outcome"] = outcome

    def cleanup(self, adapter: LeaseAdapterModel, *, lose_reply: bool = False) -> None:
        if self.state["outcome"] == "pending":
            raise Rejected("caller still using ownership")
        if self.state["cleanup_pending"]:
            adapter.release(self.identity)
            if lose_reply:
                raise LostReply("release response lost")
            self.state["cleanup_pending"] = False
