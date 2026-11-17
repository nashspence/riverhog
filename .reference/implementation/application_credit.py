"""Reference-only bounded application credit meter; no authority or persistence.

The application owner verifies original invocation receipts and owns cost/grant
settlement. This helper only computes conservative fresh useful elapsed. It is
not the shared recovery policy, a scheduler, a remote authentication layer, or a
resource receiver. See ../BUDGET.md for the complete integration contract.
"""

from __future__ import annotations

from dataclasses import dataclass


def _nonnegative(value: int, name: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be an exact nonnegative integer")
    return value


@dataclass(frozen=True)
class OriginalInterval:
    epoch: str
    sequence: int


@dataclass(frozen=True)
class VerifiedWorkAmounts:
    """Owner-verified original amounts, not a wire message or capability.

    fresh_useful_ns excludes replay, waits, optional work and overlapping child
    credit. optional_elapsed_ns includes this invocation's entire optional
    subtree once. Unknown accounting is represented by None at receive().
    """

    fresh_useful_ns: int
    optional_elapsed_ns: int

    def __post_init__(self) -> None:
        _nonnegative(self.fresh_useful_ns, "fresh useful elapsed")
        _nonnegative(self.optional_elapsed_ns, "optional elapsed")


class CohortCredit:
    """One serialized owner; active metadata bounded by existing admission.

    Every timestamp is from the root's one monotonic clock. Local flags must be
    driven by actual owned execution, never Future age, a poll, or process age.
    Accounting prefixes can arrive during execution; receive() is used only
    AFTER original receiving. A prefix is never a resource-completion signal.
    Restoring creates a new epoch with only the last durable credited total;
    earlier receipts still settle their real resources/costs outside this meter.
    """

    def __init__(
        self,
        *,
        epoch: str,
        max_active: int,
        now_ns: int,
        durable_credit_ns: int = 0,
    ) -> None:
        if not epoch or type(epoch) is not str:
            raise ValueError("an owner epoch is required")
        if type(max_active) is not int or max_active < 1:
            raise ValueError("active capacity must be positive")
        self.epoch = epoch
        self.max_active = max_active
        self.credit_ns = _nonnegative(durable_credit_ns, "durable credit")
        self._last_ns = _nonnegative(now_ns, "clock")
        self._local_useful = False
        self._local_optional = False
        self._next = 1
        self._active: dict[int, tuple[int, VerifiedWorkAmounts, int, int]] = {}
        self._optional_phases: dict[str, int] = {}
        self._cohort_start: int | None = None
        self._max_fresh_ns = 0
        self._optional_debit_ns = 0
        self._cohort_known = True
        self._cohort_credited_ns = 0

    @property
    def active_count(self) -> int:
        return len(self._active)

    def _advance(self, now_ns: int) -> None:
        _nonnegative(now_ns, "clock")
        if now_ns < self._last_ns:
            raise ValueError("root clock moved backward")
        elapsed = now_ns - self._last_ns
        if self._cohort_start is not None:
            if self._local_optional:
                self._optional_debit_ns += elapsed
        elif self._local_useful and not self._local_optional and not self._optional_phases:
            self.credit_ns += elapsed
        self._last_ns = now_ns

    def _credit_cohort(self, now_ns: int) -> None:
        if (
            self._cohort_start is None
            or not self._cohort_known
            or self._optional_phases
            or self._local_optional
        ):
            return
        available = min(self._max_fresh_ns, now_ns - self._cohort_start)
        lower_bound = max(0, available - self._optional_debit_ns)
        additional = max(0, lower_bound - self._cohort_credited_ns)
        self.credit_ns += additional
        self._cohort_credited_ns += additional

    def optional_phase(self, phase_id: str, *, starting: bool, now_ns: int) -> None:
        """Freeze credit before authorized optional execution, receive its end.

        The caller owns the real grant. End is reported only after all original
        cumulative optional costs for this phase have been received by the real
        owner (and folded via prefix() while its invocation remains active).
        A held future cleanup phase must call start BEFORE it begins execution.
        """
        if not phase_id or type(phase_id) is not str or type(starting) is not bool:
            raise ValueError("an exact optional phase is required")
        if starting:
            if phase_id in self._optional_phases:
                raise ValueError("optional phase is already active")
            if len(self._optional_phases) >= self.max_active:
                raise RuntimeError("existing optional admission is full")
        elif phase_id not in self._optional_phases:
            raise ValueError("optional phase is not active")
        self._advance(now_ns)
        if starting:
            self._optional_phases[phase_id] = now_ns
        else:
            started_ns = self._optional_phases.pop(phase_id)
            if self._cohort_start is not None:
                # This conservative overlap debit also covers cleanup whose
                # execution interval was already received. It is NOT an actual
                # optional-cost charge; cost settlement remains the root's job.
                self._optional_debit_ns += now_ns - max(started_ns, self._cohort_start)
            self._credit_cohort(now_ns)
            if not self._active and not self._optional_phases:
                self._cohort_start = None

    def local_state(self, *, fresh_useful: bool, optional: bool, now_ns: int) -> None:
        if type(fresh_useful) is not bool or type(optional) is not bool:
            raise ValueError("local execution flags must be booleans")
        self._advance(now_ns)
        self._local_useful = fresh_useful
        self._local_optional = optional
        self._credit_cohort(now_ns)

    def start_remote(self, *, now_ns: int) -> OriginalInterval:
        if len(self._active) >= self.max_active:
            raise RuntimeError("existing execution admission is full")
        self._advance(now_ns)
        if self._cohort_start is None:
            self._cohort_start = now_ns
            self._max_fresh_ns = 0
            self._optional_debit_ns = 0
            self._cohort_known = True
            self._cohort_credited_ns = 0
        sequence = self._next
        self._next += 1
        self._active[sequence] = (0, VerifiedWorkAmounts(0, 0), 0, now_ns)
        return OriginalInterval(self.epoch, sequence)

    def prefix(
        self,
        interval: OriginalInterval,
        amounts: VerifiedWorkAmounts,
        *,
        prefix_sequence: int,
        now_ns: int,
    ) -> bool:
        """Fold an exact authenticated cumulative prefix, without ending work."""
        if type(interval) is not OriginalInterval or type(amounts) is not VerifiedWorkAmounts:
            raise ValueError("an original interval and verified amounts are required")
        _nonnegative(interval.sequence, "interval sequence")
        _nonnegative(prefix_sequence, "prefix sequence")
        if interval.sequence == 0 or prefix_sequence == 0:
            raise ValueError("sequences must be positive")
        if interval.epoch != self.epoch:
            return False
        if interval.sequence >= self._next:
            raise ValueError("interval was never issued by this owner")
        previous = self._active.get(interval.sequence)
        if previous is None:
            return False
        sequence, prior, prior_conservative_fresh, prior_received_ns = previous
        if prefix_sequence == sequence:
            if amounts != prior:
                raise ValueError("conflicting original prefix")
            return False
        if prefix_sequence < sequence:
            raise ValueError("stale prefix sequence")
        if (
            amounts.fresh_useful_ns < prior.fresh_useful_ns
            or amounts.optional_elapsed_ns < prior.optional_elapsed_ns
        ):
            raise ValueError("cumulative amounts moved backward")
        if amounts.optional_elapsed_ns > prior.optional_elapsed_ns and not self._optional_phases:
            raise ValueError("optional work was not announced before execution")
        self._advance(now_ns)
        conservative_fresh = prior_conservative_fresh + min(
            amounts.fresh_useful_ns - prior.fresh_useful_ns,
            now_ns - prior_received_ns,
        )
        self._active[interval.sequence] = (
            prefix_sequence,
            amounts,
            conservative_fresh,
            now_ns,
        )
        self._max_fresh_ns = max(self._max_fresh_ns, conservative_fresh)
        self._optional_debit_ns += amounts.optional_elapsed_ns - prior.optional_elapsed_ns
        self._credit_cohort(now_ns)
        return True

    def receive(
        self,
        interval: OriginalInterval,
        amounts: VerifiedWorkAmounts | None,
        *,
        now_ns: int,
    ) -> bool:
        """Fold a positively received original once, without receiving it here.

        False means no credit mutation: an old epoch or a previously folded
        sequence. The real owner must still verify an exact duplicate receipt;
        this bounded meter deliberately retains no lifetime receipt history.
        """
        if type(interval) is not OriginalInterval:
            raise ValueError("an original interval is required")
        _nonnegative(interval.sequence, "interval sequence")
        if interval.sequence == 0:
            raise ValueError("interval sequence must be positive")
        if amounts is not None and type(amounts) is not VerifiedWorkAmounts:
            raise ValueError("verified work amounts are required")
        if interval.epoch != self.epoch:
            return False
        if interval.sequence >= self._next:
            raise ValueError("interval was never issued by this owner")
        if interval.sequence not in self._active:
            return False
        if amounts is None:
            self._advance(now_ns)
            self._cohort_known = False
        else:
            self.prefix(
                interval,
                amounts,
                prefix_sequence=self._active[interval.sequence][0] + 1,
                now_ns=now_ns,
            )
        del self._active[interval.sequence]
        if not self._active and not self._optional_phases:
            self._cohort_start = None
        return True
