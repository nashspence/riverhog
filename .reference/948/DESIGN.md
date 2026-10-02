# #948: bounded extension control and deferred execution

Proposed external reference under #903, not accepted repository authority. Read
[the handoff](../HANDOFF.md). This revision supersedes the design/model handoff at
`6d5e3985fb9c781052594641773ba36ea1e05b30`; history remains intact. Earlier conversation
is intent calibration. #948 and subsequent maintainer decisions own scope.

## Recommendation and minimum v1 change

Keep Stove0 content-opaque. Make extension execution durably pollable, and let the
component runtime defer dispatch until it can actually run. Do not turn Stove0 into
a resource scheduler, or make every deployment implement a lease service.

The governing rule from #948 is:

> Stove0’s scheduler may synchronously perform only bounded control-plane operations
> against extensions: accept/refresh, poll, and cancel. Actual observer, target, or
> effect execution must occur outside the Stove0 scheduler thread.

The minimum useful contract change is asymmetric:

- Transform and ordinary workflow-effect targets already have a suitable job
  protocol. Fix their runtime queue/dispatch/cancellation/recovery behavior without
  introducing another target state machine.
- Observers and the separate artifact-free departure-effect interface need pollable
  runtime delivery around their existing typed terminal results/receipts.
- Stove0's affected callers need resumable advancement and durable cancellation.
  This includes nested observation and preview/automatic-admission paths, not just
  the main target call. No new GPU-specific `WorkPhase` is needed.

The reusable Python admission boundary can be small and local to support code.
Backend resource names, scheduling policy, broker credentials, permits, renewal
mechanisms and priorities must not become fields in sealed workflows or mandatory
public scheduler interfaces. Optional polling hints and a generic `Job[T]` framework
are not prerequisites. Default admission needs no external service; execution starts
promptly when local capacity is available, never inline in the control handler.

## Meaning of bounded and pending

This is a call-graph rule, not just a thread-placement rule. Dispatching a future and
then waiting for its result, sleeping through retries, or blocking on a semaphore
still ties up the caller. Bound each control operation and the aggregate contact
work in a scheduler pass; a backlog of slow endpoints must not exhaust the whole
pass. Use due scheduling and per-component isolation/backoff rather than a retry
loop in a client. A network timeout does not cancel remote work.

Pending is not literally resource-free: durable metadata, bounded bookkeeping,
and authority maintenance remain necessary. It must not require one payload worker,
plaintext workspace, retrieval session or GPU permit per waiting job. Short bounded
admission probes are control overhead, not parked payload executors. Execution
runtime deadlines must not start at queue acceptance; indefinite admission delay
is not media failure. An explicit operator queue deadline is a separate policy.

Descriptor reads and deterministic preflight can remain synchronous only as bounded
metadata validation: no content reads, scarce-resource acquisition, execution, or
waiting for admission. Resource-dependent analysis belongs in an observation/job.
This is the proposed interpretation of #948's control-operation list, to confirm in
the owning issue rather than silently deleting existing descriptor/preflight APIs.

## Durable acceptance and component-owned dispatch

Stove0 persists exact invocation identity before its first remote command. The
extension returns accepted/queued only after durable identity and initial status
are recoverable. Make acceptance atomic or cover every partial-write recovery
window. Queue saturation is a declared retryable refusal before acceptance, not a
fake queued response or eviction of accepted work.

Keep queued jobs dormant. Repeated PUT refreshes/replays the same invocation and
may wake a dispatcher; it neither starts a duplicate future nor increments an
execution attempt. GET only observes. Polling unchanged status must not generate
semantic progress or an endless WORK_UPDATED stream. Completed results bypass
admission entirely and replay exactly. Bound queue rows/bytes and volatile state;
retain receipts/tombstones for the supported replay horizon.

A component-local dispatcher performs bounded work without requiring a new daemon:

1. Reserve immediately usable local dispatch capacity without waiting. Do not first
   acquire a GPU lease and then place it in an executor's backlog.
2. Try admission with a bounded deadline outside transactions and global locks.
   Deferred admission releases the short reservation and is reconsidered later.
   Broker failure fails closed with operational diagnostics, not inapplicability.
3. A grant is an owner-specific permit, not an availability Boolean. Check exact
   invocation/attempt ownership, cancellation, authority freshness and launch
   capacity again. Release stale grants belonging to this probe. Reject and
   reconcile a foreign-owner grant without freeing another consumer's permit.
4. Persist the may-have-started boundary before any payload or effect can execute.
   Launch with the reserved slot. Clean up a proven launch failure; an ambiguous
   launch is not proof that execution never happened.
5. Release resources only after consumers and child processes stop. Renew and
   supervise an expiring permit while it is in use, when that backend requires it.

A scheduler adapter that registers jobs must deduplicate repeated probes and withdraw
canceled registrations. An unknown grant after timeout needs reconciliation/expiry,
not an unbounded pile of abandoned requests. A probe timeout is not proof that its
thread exited: production must bound actual in-flight calls as well as bookkeeping.
The fixture models probe expiry, not transport cancellation or containment.

A non-expiring local lock does not need a renewal protocol. A renewable lease does.
Neither a process crash nor a TTL proves an FFmpeg child stopped; deployment-specific
containment/fencing must establish safe reallocation. Quarantine when exclusivity
cannot be established. These are obligations of the selected adapter/supervisor,
not a requirement to build GPU fencing or distributed exactly-once execution into
Stove0. Start with invocation-level admission. Per-file release/reacquisition and
mid-encode preemption are deliberately outside the minimum change.

## Keep runtime identity separate from semantic evidence

### Transform and ordinary workflow-effect targets

Retain `TargetJobStatus` and existing PUT/GET/cancel operations. Queued records must
remain dispatchable after acceptance and restart. A known-unstarted effect is safe
to admit later; a possibly committed effect is not safe to execute again merely
because a permit is available. Use the existing interrupted/uncertain-effect
semantics rather than introducing a new public target enum just for uniformity.
Resource unavailability must not be reported as retryable terminal execution failure:
that is a different lifecycle outcome and does not mean ordinary queue waiting.

### Observers

Add a runtime envelope with exact execution identity, semantic request identity,
attempt, lifecycle state and optional terminal `ContentObservationResult`. Pending
states are never `ContentObservationEvidence`. The existing result's observed,
inapplicable, failed and canceled outcomes keep their meanings; a terminal envelope
means delivery is complete, not that observation succeeded.

Use one idempotent accept/refresh, observational poll and cancel pattern; a concrete
candidate is PUT/GET `/v1/observations/{observation_job_id}` plus cancellation. Exact
names and status representation are integration choices. Revise the pre-v1 baseline
rather than preserving a hidden synchronous compatibility path.

Do not use semantic `request_id` alone as execution identity. Current invocation
separately carries claim/fence authority. Bind a domain-separated job identity to
the immutable non-secret invocation declaration: request, claim generation, selected
implementation and exact evidence/workspace-protection binding. Refreshable secrets
stay outside that identity and outside durable records. A new claim generation
must not overwrite a live job. Reusing cached facts requires explicit semantic and
authorization validation, not pretending an old execution ran under a new claim.

The implementation author's `observe(request, runtime)` can remain synchronous
inside the component executor. Only its invocation/delivery becomes asynchronous.
Validation of typed results still uses the existing acceptance path.

### Artifact-free departure effects

Keep `DepartureEffectIntent`, stable `departure_id` and `DepartureEffectReceipt`.
Add pollable status around the receipt, retaining the existing identity and scope;
do not manufacture artifact claims or transform plans merely to share mechanics.
Pending/canceled/uncertain is not a successful withdrawal receipt. Support explicit
cancellation only with observable unresolved/terminal handling; required withdrawal
must not silently disappear because a policy changed or a queue was pruned.

Durable receipt replay and target-owned effect reconciliation remain mandatory.
HTTP 202 alone supplies none of these guarantees. Returning a validated status with
HTTP 200 is consistent with existing targets; update executable status/error
contracts and clients together instead of changing status codes for appearance.

## Resumable Stove0 callers and authority maintenance

Store continuation and operational next-contact state apart from semantic identities.
A pending observation leaves work observing; a pending target leaves it queued.
Advance other ready independent observations rather than repeatedly selecting only
the first incomplete one. Dependencies still wait for valid facts. Persist completed
nested evidence and resume planning instead of redoing all earlier observations.

Preview must become resumable wherever used, including automatic classification
admission, operator preview, evaluation callers, and `/v1/work`'s current re-preview
check. A pending preview is not a failed preview and is not executable approval.
Do not eliminate accepted-preview digest comparison just to avoid waiting: bind or
resume exact revalidation and defer work creation until that validation completes.
Reuse a validated stored preview only under explicit existing authority rules.

Preserve preview read-only custody: no target execution, output-write authority,
collection publication or source retirement. Do not abandon the preview claim in
a synchronous `finally` while accepted observations still need it. Cancellation
must reach accepted observer jobs and settle custody before final cleanup.

Decouple claim maintenance from remote polling. At the audited base, renewal runs
inside `coordinator.step`; skipping that whole step during a long poll backoff can
expire the claim. Make maintenance independently due, or wake for maintenance
without contacting the extension. Bound cumulative scan delay against those duties.
Keep capability/callback refresh working while queued and running; revalidate
fresh authority before dispatch after restart. GET/status alone does not renew
permission to execute. No bearer, callback or lease tokens belong in durable jobs.

Each component owns its job store and Stove0 owns its continuations. Share focused
support mechanics, not databases or imports between implementation packages. Do not
replace typed evidence/publication rules with a single generic job framework.

## Cancellation, recovery and already-proven completion

Persist cancellation intent before depending on remote acknowledgment. Retry cancel
until reconciled; ordinary refresh must not override it. A lost first PUT response
leaves submission uncertain: converge cancellation against that exact identity
(or an authorized tombstone), so a delayed PUT cannot resurrect canceled work.

| Durable knowledge | Required consequence |
| --- | --- |
| Accepted, definitely unstarted | Remain queued after restart; cancel without acquiring capacity or a payload worker |
| Probe outstanding | Cancellation invalidates launch; reconcile any late owned grant |
| Start marker exists, outcome absent | May have executed, even if the crash was just before launch |
| Replay-safe interruption, no cancel requested | Resume only with old execution contained, fresh authority and existing exact checkpoint rules |
| Cancel requested before crash | Preserve it across restart; never convert it into permission to resume payload work |
| External effect possibly committed | Remain unresolved; no generic replay and no fabricated canceled receipt |
| Exact completed evidence exists | Replay it; a later cancellation, stale attempt or resource-lease loss does not undo a committed result |

Recovery must inspect completed/publication checkpoints before deciding that a
canceled transform can simply be abandoned. For interrupted observer/transform work
known stopped with no completed result to reconcile, cancellation needs no restarted
payload. For uncertain effects, stopping proves no further activity, not absence
of an earlier commit. Reconcile by the existing effect/receipt authority.

Resource permission and completion authority are distinct. A lost GPU lease forbids
continued consumption; it does not erase an independently proven completed result.
Conversely, a valid GPU lease is not Riverhog publication authority. Preserve the
existing success-wins-over-late-cancel/cleanup behavior. Cleanup failure must not
change success into a replayable failure, and resource release must still converge.

## Witness, exclusions and evidence

Eventual integration must exercise the actual NVIDIA encode target with a synthetic
other application withholding its external lease for an indeterminate interval.
Several jobs remain durable, inspectable and cancelable without waiting FFmpeg
processes or payload workers. Unrelated work continues. Restart during the wait,
refresh authority, then grant one lease and observe eligible execution. Cancel a
waiting job and prove it never starts on a late grant. Health/version/preflight
requests must not acquire the lease. Pending time must not consume encode timeout.

The 36 tests in this reference are an explicit-interleaving SQLite model with fake
permits, not that end-to-end witness. They do not exercise actual threads, HTTP,
NVENC, capabilities, publication, preview traversal or distributed transactions.
`workers_stopped`, `no_effect` and `completion_proven` are fixture assertions, not
proposed trusted Boolean fields in any runtime/public API. Production must derive
those facts from its real supervision and exact evidence. Model `uncertain` does
not propose another target wire state. See [VALIDATION.md](VALIDATION.md).

Do not solve #948 by a blocking FFmpeg wrapper, more workers, a `waiting_for_gpu`
field, client sleep loops, or generic resource inventory/priorities/backends.
Review0 sampler APIs are independent: assess them only if the affected scheduler
call graph reaches them. No fleet scheduler, migration, preemption or general-purpose
workflow framework is part of this reference's minimum recommendation.

External primary references: Python [executor/future semantics](https://docs.python.org/3/library/concurrent.futures.html)
and [RFC 9110](https://www.rfc-editor.org/rfc/rfc9110), sections 10.2.3 and 15.3.3.
These support runtime/HTTP cautions, not repository-specific design authority.
