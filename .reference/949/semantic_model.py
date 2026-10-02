"""Non-authoritative #949 semantic experiment; NOT a Riverhog adapter or wire API.

Each method models an atomic durable transition. JSON snapshots model restart
boundaries, not real fsync, transactions, network streams, or device safety.
Resource labels and byte counts are adversarial fixtures; no bytes are stored.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any


class Rejected(RuntimeError):
    """An identity, ownership, or integrity violation, not normal waiting."""


class Deferred(RuntimeError):
    """Known nonterminal resource wait. No ETA or retry deadline is implied."""


class LostReply(RuntimeError):
    """An effect committed but the caller did not record its response."""


@dataclass(frozen=True)
class ReadDemand:
    incarnation: str
    consumer: str
    object_key: str  # Fixture stands for an exact ObjectLocator, including revision.


class AdapterModel:
    def __init__(self, incarnation: str, routes: dict[str, str], existing: tuple[str, ...] = ()):
        self.state: dict[str, Any] = {
            "incarnation": incarnation, "routes": dict(routes), "objects": list(existing),
            "reads": {}, "pins": {}, "writes": {}, "events": [],
        }
        self.online: set[str] = set()

    def snapshot(self) -> str:
        return json.dumps(self.state, sort_keys=True)

    @classmethod
    def restore(cls, snapshot: str) -> AdapterModel:
        state = json.loads(snapshot)
        result = cls(state["incarnation"], state["routes"])
        result.state = state
        # Availability must be re-observed, not inferred from durable intent.
        return result

    def _fence(self, incarnation: str) -> None:
        if incarnation != self.state["incarnation"]:
            raise Rejected("incarnation changed")

    def _resource(self, key: str) -> str:
        if key not in self.state["routes"]:
            raise Rejected("unknown object route")
        return self.state["routes"][key]

    def _available(self, key: str) -> bool:
        return self._resource(key) in self.online

    def _event(self, kind: str, subject: str) -> None:
        self.state["events"].append({
            "id": str(len(self.state["events"]) + 1), "type": kind, "subject": subject,
        })

    def observe_online(self, resource: str) -> None:
        if resource not in self.state["routes"].values():
            raise Rejected("unknown resource")
        self.online.add(resource)

    def lose_resource(self, resource: str) -> None:
        self.online.discard(resource)

    def head(self, key: str) -> bool:
        self._resource(key)
        return key in self.state["objects"]  # Offline is NOT missing.

    def _read(self, demand: ReadDemand, *, create: bool) -> dict[str, Any]:
        self._fence(demand.incarnation)
        self._resource(demand.object_key)
        identity = asdict(demand)
        record = self.state["reads"].get(demand.consumer)
        if record is None:
            if not create:
                raise Rejected("unknown demand")
            record = {"identity": identity, "released": False}
            self.state["reads"][demand.consumer] = record
        elif record["identity"] != identity:
            raise Rejected("demand identity changed")
        return record

    def prepare_read(self, demand: ReadDemand) -> None:
        self._fence(demand.incarnation)
        record = self.state["reads"].get(demand.consumer)
        if record is not None:
            self._read(demand, create=False)
            return  # Includes terminal replay; never resurrect released demand.
        if not self.head(demand.object_key):
            raise Rejected("object missing")
        self._read(demand, create=True)
        self._event("read-demand-created", demand.consumer)

    def read_status(self, demand: ReadDemand) -> str:
        if self._read(demand, create=False)["released"]:
            return "released"  # Model-only terminal state, not a proposed wire literal.
        return "ready" if self._available(demand.object_key) else "requested"

    def release_read(self, demand: ReadDemand) -> None:
        # Full identity lets release fence even an as-yet-unobserved prepare.
        record = self._read(demand, create=True)
        if not record["released"]:
            record["released"] = True
            self._event("read-demand-released", demand.consumer)

    def open_read(self, demand: ReadDemand, stream: str) -> None:
        if self.read_status(demand) == "released":
            raise Rejected("released demand")
        if not self._available(demand.object_key):
            raise Deferred("read resource unavailable")
        if not self.head(demand.object_key):
            raise Rejected("object missing")
        identity = asdict(demand)
        previous = self.state["pins"].get(stream)
        if previous is not None and previous != identity:
            raise Rejected("stream identity changed")
        self.state["pins"][stream] = identity

    def close_read(self, stream: str) -> None:
        self.state["pins"].pop(stream, None)

    def begin_write(self, incarnation: str, token: str, key: str, size: int, identity: str) -> None:
        self._fence(incarnation)
        self._resource(key)
        if type(size) is not int or size <= 0:
            raise Rejected("invalid expected length")
        exact = [incarnation, key, size, identity]
        record = self.state["writes"].get(token)
        if record is not None:
            if record["identity"] != exact:
                raise Rejected("write identity changed")
            return
        if self.head(key) or any(w["identity"][1] == key and w["state"] != "aborted"
                                 for w in self.state["writes"].values()):
            raise Rejected("object already owned")
        self.state["writes"][token] = {"identity": exact, "segments": {}, "state": "active"}
        self._event("write-intent-created", token)

    def _write(self, incarnation: str, token: str) -> dict[str, Any]:
        self._fence(incarnation)
        if token not in self.state["writes"]:
            raise Rejected("unknown write")
        return self.state["writes"][token]

    def write_status(self, incarnation: str, token: str) -> str:
        record = self._write(incarnation, token)
        if record["state"] != "active":
            return record["state"]
        return "ready" if self._available(record["identity"][1]) else "requested"

    def segment(self, incarnation: str, token: str, number: int, size: int, digest: str) -> None:
        record = self._write(incarnation, token)
        if record["state"] != "active":
            raise Rejected("write not active")
        if type(number) is not int or number < 1 or type(size) is not int or size < 1:
            raise Rejected("invalid segment")
        previous = record["segments"].get(str(number))
        if previous is not None:
            if previous != [size, digest]:
                raise Rejected("segment identity changed")
            return  # A durable receipt can be replayed while media are offline.
        if sum(item[0] for item in record["segments"].values()) + size > record["identity"][2]:
            raise Rejected("length exceeded")
        if not self._available(record["identity"][1]):
            raise Deferred("write resource unavailable")
        record["segments"][str(number)] = [size, digest]

    def complete(self, incarnation: str, token: str) -> None:
        record = self._write(incarnation, token)
        if record["state"] == "completed":
            return
        if record["state"] != "active":
            raise Rejected("write not active")
        segments = record["segments"]
        if sorted(map(int, segments)) != list(range(1, len(segments) + 1)) or sum(
            item[0] for item in segments.values()
        ) != record["identity"][2]:
            raise Rejected("incomplete segments")
        if not self._available(record["identity"][1]):
            raise Deferred("completion resource unavailable")
        # Abstract commit point, NOT proof that a real filesystem made data durable.
        record["state"] = "completed"
        key = record["identity"][1]
        if key not in self.state["objects"]:
            self.state["objects"].append(key)

    def abort(self, incarnation: str, token: str) -> None:
        record = self._write(incarnation, token)
        if record["state"] == "aborted":
            return
        if record["state"] == "completed":
            raise Rejected("cannot abort committed object")
        record["state"] = "aborting"  # Fences new segments before cleanup.
        if record["segments"] and not self._available(record["identity"][1]):
            raise Deferred("abort cleanup resource unavailable")
        record["segments"] = {}
        record["state"] = "aborted"

    def delete(self, incarnation: str, key: str) -> None:
        self._fence(incarnation)
        if not self.head(key):
            return
        if self.busy(self._resource(key)):
            raise Deferred("resource still in use")
        if not self._available(key):
            raise Deferred("deletion resource unavailable")
        self.state["objects"].remove(key)

    def busy(self, resource: str) -> bool:
        read_keys = [r["identity"]["object_key"] for r in self.state["reads"].values()
                     if not r["released"]]
        pin_keys = [p["object_key"] for p in self.state["pins"].values()]
        write_keys = [w["identity"][1] for w in self.state["writes"].values()
                      if w["state"] not in {"completed", "aborted"}]
        return any(self._resource(key) == resource for key in read_keys + pin_keys + write_keys)

    def quiesce(self, resource: str) -> bool:
        """Atomic admission closure in the MODEL, not a safe-to-eject OS operation."""
        if resource not in self.state["routes"].values():
            raise Rejected("unknown resource")
        if self.busy(resource):
            return False
        self.online.discard(resource)
        return True


class ConsumerModel:
    """Caller-owned intent and independent durable cleanup obligation."""
    def __init__(self, demand: ReadDemand):
        self.state = {"demand": asdict(demand), "outcome": "pending", "cleanup_pending": True}

    def snapshot(self) -> str:
        return json.dumps(self.state, sort_keys=True)

    @classmethod
    def restore(cls, snapshot: str) -> ConsumerModel:
        state = json.loads(snapshot)
        result = cls(ReadDemand(**state["demand"]))
        result.state = state
        return result

    @property
    def demand(self) -> ReadDemand:
        return ReadDemand(**self.state["demand"])

    def prepare(self, adapter: AdapterModel, *, lose_reply: bool = False) -> None:
        if self.state["outcome"] != "pending":
            raise Rejected("consumer terminal")
        adapter.prepare_read(self.demand)
        if lose_reply:
            raise LostReply("prepare accepted")

    def settle(self, outcome: str) -> None:
        if outcome not in {"succeeded", "failed", "canceled"}:
            raise Rejected("invalid outcome")
        if self.state["outcome"] not in {"pending", outcome}:
            raise Rejected("outcome changed")
        self.state["outcome"] = outcome

    def cleanup(self, adapter: AdapterModel, *, lose_reply: bool = False) -> None:
        if self.state["outcome"] == "pending":
            raise Rejected("consumer still needs its source")
        if self.state["cleanup_pending"]:
            adapter.release_read(self.demand)
            if lose_reply:
                raise LostReply("release accepted")
            self.state["cleanup_pending"] = False


def disposition(error: Exception) -> str:
    """Semantic distinctions only: deliberately does not choose HTTP codes."""
    if isinstance(error, Deferred):
        return "wait"
    if isinstance(error, (LostReply, TimeoutError)):
        return "reconcile"
    return "fail"
