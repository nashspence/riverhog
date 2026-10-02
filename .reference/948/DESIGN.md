# #948: bounded extension control and deferred execution

**Proposed reference design, not accepted repository authority.** Read
[the handoff](../HANDOFF.md). #948 owns scope and decisions; #903 governs this
external reference. Earlier conversation sketches are motivation, not specification.

## Recommendation

Preserve Stove0's content-opaque orchestration and existing target result contracts.
Give every extension execution boundary a durable, pollable lifecycle, and keep
resource admission in the component runtime. Do not introduce a GPU scheduler into
Stove0 or a second generic workflow ontology.

The governing rule from #948 is:

> Stove0's scheduler may synchronously perform only bounded control-plane operations
> against extensions: accept/refresh, poll, and cancel. Actual observer, target, or
> effect execution must occur outside the Stove0 scheduler thread.

This is a call-graph rule, not merely a thread-placement rule. Starting a local
future and immediately waiting for it, blocking on a semaphore, or hiding a polling
loop in a client still violates it. Waiting jobs retain metadata and may require
claim maintenance; they are not literally resource-free. They must not retain a
payload executor slot, GPU lease, plaintext workspace, or open payload retrieval
solely because external admission is unavailable.

Immediate admission remains the default, but **never inline payload execution**.
Unrelated work can progress; dependent recipe branches still wait for their actual
inputs. This does not promise that an external scheduler ever grants capacity.

## What the audited base establishes

Base: `main` at `239985271c9ec24942073f4005dd18da621d0251`.
Source locations and blob identities are in [INTEGRATION.md](INTEGRATION.md).

* Target submission is already bound in Stove0 state before `put_job`. Repeated PUTs
  retain the declaration and refresh runtime/callback authority. Target `queued`
  already maps to Stove0 `queued`; no new GPU-specific work phase is needed.
* `PersistentTargetService` immediately submits new jobs, creates per-job sessions,
  and marks them running before invoking their implementation. It does not yet
  implement a dormant, externally admitted queue. Queued cancellation currently
  depends on an executor eventually observing the cancellation event.
* Recovery currently maps queued/running/canceling to interrupted; ordinary effect
  jobs then cannot automatically resume. That is conservative for a possibly
  started effect, but wrong for a new queue whose records prove execution never
  started. Acceptance and status are separate files, another crash window to close.
* Observers synchronously return terminal evidence and block on an HTTP-binding
  semaphore. Nested planning also invokes them synchronously. Preview waits on
  observation futures; automatic admission calls preview synchronously.
* Departure effects are a separate, artifact-free, receipt-only protocol. Their
  local intent is durable and retry-dated, but the call still waits for execution.

Thus a target admission callback alone cannot close #948. The observer, nested
planning, preview/admission, and departure call paths must also be reconciled.

## 1. Keep control, admission, and execution separate

Use three responsibilities, without requiring three services or processes:

| Responsibility | Work allowed | Work prohibited |
| --- | --- | --- |
| Stove0 step / component control handler | Validate bounded declarations; persist/replay identity; refresh credentials; return status; record cancellation | Wait for a resource or a payload result; sleep through retries |
| Component-local dispatcher | Select bounded due candidates; make a bounded admission probe; reserve an immediately usable local slot | Allocate a waiting worker per queued item; hold a service-wide lock through external calls |
| Component execution owner | Read inputs; run tools; maintain a granted permit; checkpoint/publish evidence; stop and clean up | Invent new workflow authority; release a device while its consumer still runs |

PUT may trigger or wake dispatch, but does not wait for it or call the payload
implementation. An in-process dispatcher is enough. A bounded probe may also be
performed as a control action if the adapter demonstrably meets the same deadline;
its ability to wait for a grant is never part of the control contract.

Metadata descriptor reads and deterministic preflight validation need an explicit
interpretation of #948's list: permit them only as bounded metadata operations.
They must not acquire execution resources, fetch content, or wait for resource
admission. Resource-dependent preflight work belongs in an observation/job, not a
hidden exception. The owning issue should confirm this interpretation before
integration; do not remove useful existing descriptor/preflight contracts blindly.

## 2. Small runtime admission boundary

The conceptual outcome is **granted permit** or **deferred**, with optional bounded
operational diagnostics. Names and exact Python signatures remain integration
choices. Inputs identify the component invocation and attempt, not a secret-bearing
`TargetJobRequest` passed indiscriminately to a scheduler plugin.

A permit is an owner-specific handle, not an `available=True` hint. Availability
checking followed by an unprotected launch is a race. Configuration maps component
invocations to a deployment's resources. Gate endpoints, credentials, priorities,
device paths, and lease tokens are not added to sealed recipe/plan identities.
A changed transformation or selected implementation still uses the existing exact
semantic identities; this is not permission to switch implementations invisibly.

A dispatch attempt proceeds as follows:

1. Reserve a local dispatch/execution slot without waiting. If none is available,
   keep the durable job queued without acquiring an external permit.
2. Probe admission with a bounded deadline, outside database transactions and global
   state locks. Deferred/unavailable releases the short reservation and schedules
   a later probe. An unavailable broker fails closed, with diagnostics, not as
   content inapplicability or successful admission.
3. On a grant, recheck cancellation, ownership generation, current declaration,
   authority freshness, and immediate launch capacity. Discard stale replies and
   release only the permit owned by that probe. Never queue a granted GPU lease in
   an executor backlog.
4. Persist the may-have-started marker before executing any payload or external
   effect; then launch the execution owner. Release on known launch failure.
5. Maintain the permit while consuming the resource, and release after verified
   stop/cleanup. Cancellation and shutdown acknowledgements do not stand in for
   stopped child processes.

A scheduler that registers requests needs stable deduplication across probes;
repeated polls must not create new external queue entries. Timeouts with an unknown
grant need adapter-level reconciliation, expiry/cancellation, and owner-specific
cleanup. A bare `try_acquire` plus `finally: release` is not a complete lease protocol.
An expiring lease requires renewal and a loss path that stops/fences consumers
before reallocation. A local lock may need no renewal. Keep these differences
inside the deployment adapter/supervisor rather than mandating one global backend.

A process crash is not proof its FFmpeg child stopped. A GPU does not enforce a
Riverhog fence. Use process containment/device-owner enforcement appropriate to the
deployment; where exclusivity cannot be established, quarantine rather than launch
another consumer. Do not claim exactly-once effects from an expiring lease.

The initial useful granularity is an invocation. Releasing/reacquiring around
individual files or encode phases is an optional component-local optimization,
not a prerequisite or a generic mid-execution suspend/resume feature for #948.

## 3. Transport and identity: share the pattern, keep typed results

### Existing transform and workflow effect targets

Keep `TargetJobStatus` and the current PUT/GET/cancel surface. `queued` means accepted
without payload execution. Make existing queued records eligible for future dispatch,
not only newly created or interrupted records. Repeated PUT does not enqueue another
future or increment an execution attempt. GET is observational; running/terminal PUTs
refresh/replay without duplicate dispatch. Terminal evidence remains immutable.

No new target state enum is needed. Operational poll hints can be additive if useful;
a local polling policy is enough initially. The default runtime still launches
promptly when both local capacity and any configured gate permit it.

### Observers

Add a separate runtime job envelope, retaining `ContentObservationResult` as terminal
semantic evidence. Proposed shape: job identity, request identity, attempt, lifecycle
state, and optional terminal result. Queued/running/canceling/interrupted are not
facts and do not become `ContentObservationEvidence`. Only a validated terminal
result follows the existing result-acceptance path.

For a concrete baseline, use idempotent PUT, observational GET, and cancel for
`/v1/observations/{observation_job_id}`. A complete envelope contains the existing
terminal result, whose `observed/inapplicable/failed/canceled` meaning is unchanged.
The reference's generic `complete` label means terminal delivery, not semantic success.
Revise the pre-v1 baseline rather than retaining an old synchronous scheduler route
or silently switching clients between synchronous and asynchronous semantics.

**Do not key durable execution solely by semantic `request_id`.** The current
invocation binds a separate `claim_id` and `fence`. Define a domain-separated
execution identity over the immutable non-secret declaration, including the request,
claim generation, selected descriptor and exact evidence/workspace-protection binding.
Keep semantic request/result identity separate. Credential refresh must not create a
new job; a different claim generation must not silently overwrite a live invocation.
Cache reuse of old semantic facts, if supported, needs explicit validation rather
than masquerading as execution under new authority.

An observer support wrapper can keep the implementation author's `observe(request,
runtime)` function synchronous inside the component executor. The HTTP handler and
Stove0 client must not execute or await that function to completion.

### Departure effects

Keep the exact artifact-free intent and existing `departure_id`. Add a pollable
status envelope around `DepartureEffectReceipt`, using the existing PUT path with
GET/cancel semantics where authorized. Pending is not a receipt. Do not require
artifact claims or transform plans for departure jobs just to reuse runtime mechanics.

Cancellation must be explicit and observable; do not turn a canceled or uncertain
required withdrawal into `complete`, or automatically discard it when a policy or
catalog view changes. Existing durable idempotency and lost-response receipt replay
must survive the transport change. Possibly committed effects remain unresolved
until target-owned reconciliation or an explicit authorized decision supplies proof.

For new envelopes, using HTTP 200 with a validated state representation follows the
existing target binding and avoids adding 202 just for appearance. HTTP 202 alone
would not supply durable identity, polling, cancellation, or completion guarantees.
Whichever binding is integrated must update its executable error/status contracts;
queue saturation is a declared retryable refusal, not a fake accepted record.

## 4. Durable Stove0 advancement, not parked scheduler threads

Persist the exact invocation/continuation before its first remote command, and
record bounded poll bookkeeping independently of semantic result identity. Maintain
per-invocation next-contact scheduling and fair, bounded scans. A large waiting
backlog should not consume every scan's network budget or create a WORK_UPDATED
stream merely because an unchanged status was seen again. Waiting is not failure,
and polling is not an execution attempt. Do not sleep inside a scheduler pass.

Main work may remain `observing` or `queued`; fine-grained remote status is a view,
not a new GPU phase. Within an observation stage, contact other ready independent
requests rather than repeatedly selecting only the first incomplete request. Dependent
stages cannot proceed with missing facts. The same applies to nested branch planning:
persist completed evidence and resumable dependencies, and yield to the scheduler
when a nested invocation is pending. Do not restart all prior observations each poll.

Make preview advancement resumable as well, including automatic classification
admission, operator preview, and evaluation callers that reuse it. Persist its
observation/preflight continuation, return pending status instead of a failed or
fabricated ready preview, and emit a `WorkflowPreview` only after its current exact
terminal validation. A pending preview is not executable approval. Preserve its
read-only scope: no target execution, output-write grant, collection publication,
or source retirement. Do not abandon the preview claim in an old synchronous
`finally` while accepted observers still need it; cancellation must converge cleanup.

**Decouple claim/capability maintenance from poll backoff.** At the audited base,
claim renewal runs in `coordinator.step`. Simply skipping that entire step until a
long retry time can expire authority. Maintain claims on a separate due schedule
(or cap wakeups to the next maintenance deadline without recontacting the extension).
Revalidate authority before dispatch. Following restart, pending jobs require fresh
runtime material; do not persist bearer/callback/lease tokens in accepted records.

The minimal reusable code is focused lifecycle/admission mechanics in a dependency-
light support package. Each component owns its durable job storage. Stove0 owns its
own continuation records. Neither imports the other's implementation or shares a
new cross-application operational database. Reuse protocol-specific validation,
publication, effect reconciliation, and workspace cleanup; do not extract one giant
new `Job[T]` framework or replace existing evidence contracts for uniformity.

## 5. Recovery and cancellation rules

| Durable knowledge | Recovery / cancellation consequence |
| --- | --- |
| Accepted, definitely not started | Stay queued across restart; cancel immediately without needing a worker |
| Probe outstanding, no start marker | Ignore stale grant after cancellation/owner change; reconcile/release that grant |
| Start marker committed; execution outcome absent | Treat as may-have-run, even if crash happened just before launch |
| Replay-safe observer/transform interrupted | Resume only under existing exact identity/checkpoint rules and fresh authority after old execution is contained |
| External effect may have committed | No generic auto-replay; preserve unresolved status and reconcile by durable effect identity |
| Cancel requested while running | Remain canceling until execution/children stop; a proven commit may win the race |
| Durable terminal evidence exists | Replay exact evidence; do not overwrite it with a late cancel/error/old-attempt reply |

Combine acceptance and initial status atomically, or implement recovery for every
partial-write window. Bound pending storage and in-memory scheduling metadata. Reject
excess new work before acceptance; never silently evict accepted work. Terminal
retention must preserve needed deduplication, especially side-effect receipts.
Resource/authority loss cannot be reclassified as bad media. Waiting diagnostics
belong to runtime status, not sealed facts, plans, or receipts.

## Basic test witness and limits

Use the actual NVIDIA encode target in eventual integration tests, with an external
lease service held by a synthetic other application. Withhold the grant without a
promised acquisition time. Accept several encode invocations; their state must be
durable, inspectable and cancelable with no FFmpeg process, payload worker, or GPU
permit held for the wait. Unrelated observer/target/effect work must keep advancing.
Restart while waiting, refresh authority, then release one lease and observe one
eligible encode start and complete normally. Never use health/version probes as a
lease acquisition path.

`lifecycle.py` and `test_lifecycle.py` are an executable interleaving model of the
critical lifecycle rules, with SQLite reopening and fake permits. They are not an
implementation or qualification of that end-to-end witness. The model intentionally
omits HTTP, actual processes, capability validation, publication, preview traversal,
and distributed transactions. Integration evidence remains necessary.

## Rejected shortcuts / deliberately deferred scope

Do not close this issue with a blocking FFmpeg wrapper, increased worker count,
sleep/backoff inside a client, or a new `waiting_for_gpu` field. Those relocate or
hide waiting rather than establishing the control boundary. Similarly, a client
socket timeout is not proof remote work was canceled. Python futures cannot cancel
already-running calls by assertion; a real blocking adapter must have a bounded
transport/containment design.

Do not build resource inventory, priorities, scheduler backends, live GPU migration,
mid-encode preemption, or a generic fleet scheduler here. A deployment adapter can
use such systems later. Review0 samplers are adjacent GPU consumers, but changing
their independent API is not silently authorized by this reference; assess scope
in #948 if they enter the affected scheduler call graph.

External primary references: Python's [executor/future semantics](https://docs.python.org/3/library/concurrent.futures.html)
and [RFC 9110](https://www.rfc-editor.org/rfc/rfc9110), sections 10.2.3 and 15.3.3.
These support transport/runtime cautions, not repository-specific design authority.
