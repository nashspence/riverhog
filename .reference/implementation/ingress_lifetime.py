"""Reference #998 composition cuts; not an installed application implementation.

One bounded pass retains its original worker and result receiver. An original
storage close must be positively received before a claim's files are removed.
No persistent replay policy or generic tool checkpoint is implemented here.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from threading import RLock, Thread
from typing import Protocol

from a_riverhog_ftp_spool.operational_state import FtpOperationalState
from a_riverhog_ftp_spool_client.events import ATTEMPT_FAILED, validate_ftp_event

ATTEMPT_EVENT_INDEX = (
    "CREATE INDEX lifecycle_events_kind_sequence_idx ON lifecycle_events "
    "(json_extract(event_json, '$.type'), sequence DESC)"
)


class LifetimePending(RuntimeError):
    """The original operation or receiver remains unsettled."""


@dataclass(frozen=True)
class PassOutcome:
    value: object = None
    error: BaseException | None = None


_retained_passes: set[PassLifetime] = set()
_retained_closes: set[CloseBeforeRetire] = set()
_registry_lock = RLock()


class PassLifetime:
    """One non-daemon pass, no hidden queue and no blocking status operation.

    The receiver is captured once at admission. Receiving it is a private original
    completion path, separate from whether the submitting HTTP caller still exists.
    Its owner must implement exact idempotent reconciliation after a lost reply.
    """

    def __init__(
        self,
        operation: Callable[[], object],
        receiver: Callable[[PassOutcome], None],
    ) -> None:
        self._lock = RLock()
        self._operation = operation
        self._receiver = receiver
        self._outcome: PassOutcome | None = None
        self._received = False
        self._receiving = False
        self._receive_failed = False
        self._started = False
        self._start_failed = False
        self._worker = Thread(target=self._run, name="ftp-original-pass", daemon=False)
        with _registry_lock:
            _retained_passes.add(self)

    @property
    def original_worker(self) -> Thread:
        return self._worker

    @property
    def retained(self) -> bool:
        with _registry_lock:
            return self in _retained_passes

    def start(self) -> None:
        with self._lock:
            if self._started or self._start_failed:
                raise LifetimePending("original start has already been attempted")
            try:
                self._worker.start()
            except BaseException:
                # An interrupted start is not proof that the original never ran.
                self._start_failed = True
                raise
            self._started = True

    def _run(self) -> None:
        try:
            outcome = PassOutcome(value=self._operation())
        except BaseException as error:
            outcome = PassOutcome(error=error)
        with self._lock:
            self._outcome = outcome

    def status(self) -> str:
        with self._lock:
            if self._received:
                return "received"
            if self._receive_failed:
                return "receiving-unknown"
            if self._start_failed:
                return "start-unknown"
            if not self._started:
                return "admitted"
            if self._worker.is_alive() or self._outcome is None:
                return "running"
            return "ready-to-receive"

    def receive(self, *, retry_original: bool = False) -> bool:
        """Receive only after actual thread termination; never wait for progress."""
        with self._lock:
            if self._received:
                return True
            if self._start_failed:
                raise LifetimePending("original start needs original factory reconciliation")
            if not self._started or self._worker.is_alive() or self._outcome is None:
                return False
            self._worker.join(timeout=0)
            if self._receiving or (self._receive_failed and not retry_original):
                raise LifetimePending("original result receiver needs explicit retry")
            self._receiving = True
            outcome = self._outcome
        try:
            self._receiver(outcome)
        except BaseException:
            with self._lock:
                self._receive_failed = True
                self._receiving = False
            raise
        with self._lock:
            self._receiving = False
            self._received = True
            self._outcome = None
            self._receive_failed = False
        with _registry_lock:
            _retained_passes.discard(self)
        return True


class Closeable(Protocol):
    def close(self) -> None: ...


class CloseBeforeRetire:
    """Retain the exact owner until its close and dependent retirement return.

    A refused/failed close never invokes retirement and is not implicitly retried.
    Normal close return is the close owner's positive receiving contract. Callers
    whose close returns a pending outcome must adapt it to raise LifetimePending.
    """

    def __init__(self, original: Closeable, retire: Callable[[], None]) -> None:
        self.original = original
        self._retire = retire
        self._lock = RLock()
        self.closed = False
        self.retired = False
        self._close_failed = False
        self._active = False
        with _registry_lock:
            _retained_closes.add(self)

    @property
    def retained(self) -> bool:
        with _registry_lock:
            return self in _retained_closes

    def settle(self, *, retry_original: bool = False) -> None:
        with self._lock:
            if self.retired:
                return
            if self._active or (self._close_failed and not retry_original):
                raise LifetimePending("original close needs explicit receiving")
            self._active = True
        try:
            if not self.closed:
                try:
                    self.original.close()
                except BaseException:
                    self._close_failed = True
                    raise
                self.closed = True
                self._close_failed = False
            self._retire()
            self.retired = True
            with _registry_lock:
                _retained_closes.discard(self)
        finally:
            with self._lock:
                self._active = False


class IngressLifetime:
    """The application retains the pass, original receiver, resources and API.

    The runtime owns scheduled and management passes through the same bounded slot.
    The ASGI scheduler may stop awaiting a pass; that does not settle this owner.
    """

    def __init__(
        self,
        api: Closeable,
        *,
        resources_received: Callable[[], bool],
        receive_pass: Callable[[PassOutcome], None],
    ) -> None:
        self._lock = RLock()
        self._stopping = False
        self._resources_received = resources_received
        self._receive_pass = receive_pass
        self._pass: PassLifetime | None = None
        self._api = CloseBeforeRetire(api, lambda: None)

    @property
    def original_pass(self) -> PassLifetime | None:
        return self._pass

    def submit(self, operation: Callable[[], object]) -> PassLifetime:
        with self._lock:
            if self._stopping:
                raise LifetimePending("ingress admission is stopped")
            if self._pass is not None and self._pass.status() != "received":
                raise LifetimePending("the bounded original pass slot is occupied")
            selected = PassLifetime(operation, self._receive_pass)
            self._pass = selected
            selected.start()
            return selected

    def begin_stop(self) -> None:
        with self._lock:
            self._stopping = True

    def status(self) -> dict[str, object]:
        with self._lock:
            return {
                "admission": "stopped" if self._stopping else "open",
                "pass": None if self._pass is None else self._pass.status(),
                "api_close_received": self._api.closed,
            }

    def receive_pass(self, *, retry_original: bool = False) -> bool:
        with self._lock:
            selected = self._pass
        return selected is None or selected.receive(retry_original=retry_original)

    def settle_stop(self, *, retry_api_close: bool = False) -> bool:
        with self._lock:
            if not self._stopping:
                raise LifetimePending("stop admission before closing the API")
            if self._pass is not None and self._pass.status() != "received":
                return False
        if not self._resources_received():
            return False
        self._api.settle(retry_original=retry_api_close)
        return True


def may_remove_unregistered_candidate(
    claim_root: Path, *, all_original_resources_received: bool
) -> bool:
    """A finalized original claim intent survives any registration uncertainty.

    This is an application cleanup guard, not authority to traverse/delete files.
    The existing owner must still verify its exact path and original resources.
    """
    if not all_original_resources_received:
        return False
    try:
        (claim_root / "claim.json").lstat()
    except FileNotFoundError:
        return True
    return False


def cold_retry_seed(state: FtpOperationalState) -> int:
    """Derive one volatile seed from an existing accepted outcome, without writes.

    The application schema admits the index once; never create it on this path.
    Mandatory public lifecycle events retain their original meaning and cadence.
    The index avoids replaying/scanning historical events to reconstruct a cursor.
    """
    row = state.one(
        "SELECT event_json FROM lifecycle_events INDEXED BY lifecycle_events_kind_sequence_idx "
        "WHERE json_extract(event_json, '$.type') = ? ORDER BY sequence DESC LIMIT 1",
        (ATTEMPT_FAILED,),
    )
    if row is None:
        return 0
    event = validate_ftp_event(json.loads(str(row[0])))
    if event.type != ATTEMPT_FAILED:
        raise ValueError("original lifecycle outcome type changed")
    claim = state.one("SELECT ordinal FROM claims WHERE claim_id = ?", (event.subject,))
    return 0 if claim is None else int(claim[0])


def retry_page(
    state: FtpOperationalState, *, volatile_after: int, limit: int
) -> tuple[tuple[int, str], ...]:
    """Same bounded seek/wrap selection, with no durable scheduling mutation."""
    if type(volatile_after) is not int or volatile_after < 0:
        raise ValueError("retry hint must be a nonnegative integer")
    if type(limit) is not int or limit < 1:
        raise ValueError("retry page needs an explicit positive bound")
    rows = state.all(
        "SELECT ordinal, claim_id FROM claims WHERE ordinal > ? ORDER BY ordinal LIMIT ?",
        (volatile_after, limit),
    )
    if len(rows) < limit:
        rows.extend(
            state.all(
                "SELECT ordinal, claim_id FROM claims WHERE ordinal <= ? ORDER BY ordinal LIMIT ?",
                (volatile_after, limit - len(rows)),
            )
        )
    return tuple((int(row[0]), str(row[1])) for row in rows)


def dispatch_disposition(
    *,
    entry: str,
    return_code: int | None,
    original_wait_received: bool,
) -> str:
    """Hard-cut retry decision for an arbitrary command with no effect contract.

    ``not-entered`` is positive authority from the original launch owner, never an
    inference from an exception or missing PID. A nonzero received exit is failed,
    with effects unproven; it never authorizes another invocation automatically.
    """
    if entry not in {"not-entered", "entered", "unknown"}:
        raise ValueError("dispatch entry classification is invalid")
    if entry == "not-entered":
        if return_code is not None:
            raise ValueError("a non-entered command has no exit code")
        return "retryable-before-entry"
    if entry == "unknown" or not original_wait_received or return_code is None:
        return "uncertain"
    return "completed" if return_code == 0 else "failed-effects-unproven"
