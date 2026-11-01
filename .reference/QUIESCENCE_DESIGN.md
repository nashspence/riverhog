# Durable admission, draining, and exact terminal outcomes

Proposed contract for #984 at `533ac4ee06def4692288c39c9279bf9387525a3e`.
This is an owner-local design, not an implemented fix or provider qualification.

## 1. Selected mechanism and honest liveness boundary

Use **durable per-effect admission and outcome records in the existing catalog,
close the construction's admission gate, drain every previously armed effect,
adjudicate terminal rights against the reconciled results, then perform bounded
provider and catalog cleanup**. The upload service owns this state. Existing
archive maintenance schedules its work. No additional database, scheduler,
provider lock object, archive format, or custody authority is introduced.

There are two independent predicates:

- **Outcome known:** a session, part or immutable object is observed/acknowledged.
- **Execution closed:** the admitted invocation and all of its delegated attempts
  can no longer perform a future provider mutation.

Neither implies the other in general. In particular a valid observed object is
important custody evidence, but a negative HEAD/list is not proof that a delayed
begin/complete request cannot still act. A database epoch rejects future admission;
it does not revoke an already dispatched HTTP request. A released process lock or
terminated process does not establish remote execution closure.

**Selected liveness policy:** ambiguous remote effects remain durable and block
terminal success. Do not automatically clear them by lease expiry, timeout, an empty
provider listing, or a new worker's arrival. Normal fully acknowledged calls drain
and cleanup completes; an irrecoverably lost S3 begin/complete response may require
provider-specific, evidenced reconciliation and may remain pending indefinitely.
This is a deliberate safety/availability choice, not a claim of automatic remote
quiescence that the maintained provider APIs cannot supply. No force-clear endpoint
or undocumented provider fence is selected. Integration can implement this complete
fail-closed contract without researching another storage mechanism.

The original issue's two schedules are resolved differently. An armed begin may
finish, but its next part/complete is a new admission and is rejected after gate
closure. An already armed complete may finish and seal: retain the exact seal;
ordinary cancel must then refuse rather than erase it.

## 2. Durable records, immutable identities, and atomicity

Add current-only schema through the server's existing StateSchema/Alembic owner.
Operational records are not reconstructible from archive authority; keep them
through supported upgrades and interrupted cleanup. Never silently initialize over
missing pending state. Private preparation, keys and provider tokens are not logs.

| Owner-local record | Retained authority |
| --- | --- |
| Construction write control | Original collection/create identity, delivery context, store configuration and storage incarnation, exact immutable archive prefix, admission epoch/open-or-closed gate, active terminal ID, indexed unresolved count/sequence allocation. |
| Volume preparation | Exact accepted volume/plan digest/path, original encryption state, expected stored length/content type/identity assertions, raw/pack context, selected provider session when known. It exists BEFORE provider begin. |
| Armed effect | Construction, volume/resource, monotonically unique call ID, admission epoch, worker/process incarnation and one-use dispatch nonce, operation kind, exact bounded arguments/commitments, predecessor checkpoint identity, and outcome/closure state. |
| Terminal intent | Exact normalized request and preimage commitment, kind, selecting principal/key and original permission context, original discard plan and challenge/expiry where applicable, frozen scope/frontiers, original result fields, phase, bounded cleanup cursor/grant, confirmed outcome. |

Terminal intent/outcome must NOT cascade with `CollectionUploadRecord`. Retain a
small collection-owned tombstone after row teardown so lost replies replay the
original result. Retention/disposal belongs to the existing operational-state owner;
no new TTL can turn uncertain work into absent state. Never reuse the retired
prefix/incarnation or interpret a syntactically plausible challenge as proof that
this particular operation succeeded. A different request is not an exact retry.

One canonical deterministic terminal request ID can be derived from construction,
kind and complete normalized request (including exact discard challenge). Compare
retained preimages too. For cancel the construction identity disambiguates the
request; for a fresh discard after reconfirmation, the new challenge gives a new
identity. Resolve a selected exact request BEFORE testing its original challenge's
expiry again. New selections still need an unexpired matching plan. A conflict
between concurrently selected cancel and discard does not change the winning kind.

The mutable effect table has bounded outstanding rows; completed per-part facts
are merged/compacted into their existing checkpoints in the same transaction as
settlement. Do not build a forever-growing second history of every network retry.
Retain the last exact call/outcome comparison needed to reject duplicate/conflicting
reports, and the original sequence/frontier needed by a selected drain. Unresolved
calls cannot be compacted or evicted. Underlying part/checkpoint growth remains
owned and qualified by its existing operation; this change does not certify it.

## 3. Lock order and multi-worker admission

For operations needing those rows, use **permission/processing-claim guard ->
upload construction -> terminal intent -> volume -> effect**, with stable IDs
within each class. Never acquire an earlier class while holding a later class.
The current checkpoint merge takes volume before upload; reverse that order in
the coordinated change. Call outcome reporting needs upload/volume/effect, not a
fresh claim lock under an already held upload lock.

PostgreSQL uses short row locks/conditional epochs with bounded contention.
SQLite uses the existing `sqlite_immediate_writer` mechanism before reads; a
`FOR UPDATE` spelling is not a SQLite writer fence. Failed transactions discard
ORM state; retry the exact database transition, not the provider mutation.
No network request, body wait, crypto work, worker join or sleep runs inside a
shared writer transaction. Session advisory locks are not remote-effect fences.

Initialization is single-flight per volume. Multiple part lanes may run concurrently
under configured per-construction/per-volume/global limits, but one part identity
has one potentially executing invocation. Same-part exact duplicates get existing
progress/busy; differing bytes/arguments conflict. Completion is single-flight and
requires original part coverage/checkpoint plus no active part writes for that
volume. No begin/complete replay can silently allocate a replacement session.

Before begin, render and persist the original encryption/preparation state without
provider effects. Split `RawVolumeUploader.open` and its pack counterpart's current
begin-then-save ordering. An intent without a write token is explicit, not a fake
`RawUploadCheckpoint` with an invented token. A lost begin response must never
cause new encryption state, new plaintext read authority or a second begin.

Each effect has these transitions:

```
PREPARED --atomic current-gate admission--> ARMED
ARMED --trusted execution-closed + exact result--> SETTLED
ARMED --timeout/death/ambiguous error-----------> UNCERTAIN (still unresolved)
```

PREPARED has no right to invoke the provider. ARMED is durably committed before
any delegating call; it deliberately includes the interval before the first actual
provider effect. One private dispatch capability can be exercised once. A cold
worker may reconcile it but cannot reconstruct permission to resend it merely from
an ARMED row. If admission commit acknowledgement is lost, do not invoke a provider
until the dispatch owner has established its unambiguous one-use right; otherwise
retain the uncertain intent. No externally supplied call ID authorizes work.

A live dispatcher can report definitive no-effect only after it has prevented all
queued children from running and established that no provider request was sent.
A requested cancel or future cancellation alone is insufficient. A fully returned,
validated, single-attempt effect can report exact result and closure. Every nested
retry, HTTP adapter hop and child future is part of that obligation. Disable hidden
mutation retries or explicitly account for every attempt; an exception/HTTP timeout
is UNKNOWN unless its owning transport proves no effect. Do not catch-and-clear.

Outcome/checkpoint settlement is one database transaction. It can accept the exact
result of an older ARMED epoch even after terminal selection: rejecting it would
lose evidence of a real session or seal. It grants no new write. It does not renew
a custody lease, reopen the upload, schedule finalization, or fabricate full-member
custody while terminal adjudication is pending. Conflicting results fail closed.
A lost checkpoint response retries that exact report, not the provider command.

## 4. Terminal selection, drainage and rights

### Select without tearing down

Authenticate using the existing creator cancel/deletion-scoped orphan-discard
rules. In the short ordered transaction, validate current guards, persist the exact
request/plan, close provider admission, and freeze registration/finalization admission.
Record the finite admitted-effect frontier. Previously published payload work is
not erased. A terminal request is PENDING, never `canceled`/`discarded` yet.

Do not start destructive hidden-row cleanup at selection. Freeze/rotate the
registration execution epoch while retaining its exact reservation/cursor. Only
after terminal adjudication may the existing `fence_registration_disposal` begin
its cleaning phase. This small interlock prevents an ordinary cancel subsequently
refused for a late seal from having destroyed an otherwise recoverable registration.
It does not redesign the accepted planner/publication protocol.

Current public guards run outside the effect transaction in places today. Move the
necessary authenticated facts/predicate checks into admission/selection, not merely
a preceding route call. Persist stable attribution, not bearer credentials.

### Drain

Point/index queries select only unresolved effects at/below the frozen frontier,
in admitted count/byte batches. Local still-running workers retain their execution
slots until actually joined; remote-uncertain rows retain their durable resource
charge after local exit. Older exact result reports remain accepted and can advance
drainage. No next forward mutation is admitted under the closed gate.

Read-only reconciliation is allowed. Do not use `begin_write` as a lookup: the
maintained S3 begin does an active-session search and can CREATE if empty. A current
snapshot may add session/part/object evidence, but may not manufacture execution
closure. Positive completion is retained even when closure is unresolved. Keep all
original checkpoint/preparation state necessary to interpret its exact identity.

The quiescence proof is: closed admission gate, unchanged construction/incarnation,
no unclosed admitted forward effect, no untracked constructor/finalizer/copy writer,
and no live local dispatcher capable of issuing one. Use indexed existence checks
in addition to any counters, not a potentially stale counter as sole authority.
Worker fencing generations control database execution, not provider rights.

### Adjudicate cancel

After drainage, check confirmed sealed archive custody (including completion
observed remotely before its catalog merge). If present, record a stable refused
cancel result and retain objects, receipts, checkpoints and registration state.
Unknown completion also blocks successful cancellation. Do not abort/delete an
object merely to make the no-seal test true. Admission refusal is not successful
preservation and must not release producer source state.

A refused cancel does not automatically reopen an uncertain construction. Once
quiescent, the existing authorized resume owner may deliberately open a fresh
admission epoch over the same original state; old tickets never become usable.
No reopen is allowed after destructive cleanup began. Finalized collections remain
outside upload cancellation, as before.

### Adjudicate orphan discard

The original orphan-only, current scoped deletion permission and live-processing-
claim guards remain. `renew_claim` presently permits renewal of an expired claim
when it still owns output: interlock renew/restart/output acquisition with selected
terminal state under claim -> upload lock order. A preflight claim read alone would
leave a new race. Recheck applicable current permissions before each destructive
batch; revocation pauses, never widens authority. Retain the original authorization
selector/context so cleanup of tags/rows cannot accidentally broaden access.

After drain but BEFORE destructive cleanup, compare the now-known custody and
scope against the original selected challenge's terms. A new seal/custody fact not
covered by that exact plan is `needs_reconfirmation`, not implicit extra permission.
Conservatively require a fresh plan for a late seal even if the old aggregate counts
happen to match. Keep the old request/outcomes for audit; a newly authorized request
gets its own identity. Do not replace the stored original challenge in place.

If unchanged and authorized, persist cleanup-ready. Subsequent partial cleanup
necessarily changes live counts; it must NOT recompute the original challenge or
require it to remain within its initial TTL. Resume the exact selected plan and
original result fields. A new live-claim/authority conflict pauses further disposal.
An already confirmed outcome remains confirmed; retry GET/POST uses current read
permission to retrieve it, not new disposal authority.

## 5. Provider contract and unavoidable unknowns

[PROVIDER_PROFILES.md](PROVIDER_PROFILES.md) defines the concrete assumptions and
minimum current-only adapter changes. No fictional universal provider fencing API
is introduced. The chosen core contract works with explicit UNKNOWN outcomes.

A closure result is an internal adapter-owner claim supported by a qualified
execution profile, not a user-provided boolean, a self-hash or new archive receipt.
It binds call ID, exact request digest, owner/target incarnation and settled result.
A snapshot and a closure certificate must be different types. An adapter without
closure evidence for an ambiguous call returns UNKNOWN; the catalog row stays.

The generic async/network boundary cannot simultaneously promise immediate cleanup
and safety for a request with no result or provider-side revocation proof. A delayed
begin before its first visible effect and a request that never arrived have the
same catalog state and same empty listing. The former can still create a session.
The model demonstrates this distinction. A timeout value or repeated empty listing
does not distinguish them. Operator reconciliation must supply real provider-owned
closure evidence; it is not an override that declares inconvenient work stopped.

## 6. Bounded cleanup, confirmation and optional effects

Only quiescent, authorized cleanup-ready state permits destructive teardown.
Close creation/finalization/copy-intent admission for the entire retired prefix,
not just the upload-unit endpoint. Retain the exact store incarnation and prefix
through the last cleanup call; configuration-name reuse must not redirect it.

Cleanup advances resumably through bounded steps:

1. Abort known incomplete provider sessions, record exact responses, and verify
   provider-owned multipart cleanup. Do not abort a sealed object as if incomplete.
2. Remove authorized objects/versions in bounded provider pages, checking each
   item result and preserving partial failures. A prefix deletion must cover both
   current objects and retained versions/delete markers, and unfinished multipart
   sessions are separately accounted. No giant `delete_prefix` loop is a quantum.
3. Run the existing bounded hidden-registration cleanup and child-before-parent
   catalog/checkpoint cleanup under the selected disposition. Preserve original
   custody/closure evidence until all provider cleanup that depends on it is done.
4. In one small final transaction, verify no remaining scoped child/cleanup work,
   persist the exact terminal result/tombstone, then remove the upload owner row.
   Lost replies return that result, not zero-filled `already_absent` guesses.

For provider deletion, save a bounded exact page intent before issuing effects;
retry only identity-bound monotone abort/delete work in the frozen never-reused
namespace. A confirmed deletion is not repeated just to recount it. Uncertain
partial pages stay charged. Delayed monotone cleanup cannot revive objects or
obtain permission over another prefix; no subsequent writer may reuse this scope.
Final success means closed forward writers plus verified required cleanup, not
that every network packet on earth has ceased.

Existing `delete_prefix`/`discard_collection_archive_upload` are unbounded; replace
the terminal use with a bounded step contract. Each call is limited by item count,
serialized control bytes, provider requests and in-flight slots. Provider paging
with a stable continuation or repeated first-LIVE-page deletion is acceptable;
never rescan an ever-growing retained completed prefix. Versioned deletion checks
per-item errors. A rejected/partial provider operation cannot advance the final
catalog cursor or release its reservation. No silent unsupported-version fallback.

The mirrored store has effects in cache admission, begin, part futures, complete,
and even `find_completed_write` (it may admit cache). Register separate child
intents BEFORE either target call, under existing target ownership. The archive
lane is never hidden inside a cache token whose creation reply was lost. Optional
cache failure does not invalidate archive custody; its existing abandoning owner
may retain and drain a cache child under an explicit durable transfer. Terminal
completion can exclude that cache's physical deletion only if its own existing
contract already permits this transfer and independent resource ownership is
proven. Otherwise remain pending. Never drop a cache admission because the archive
began first/last. No new plaintext copy or protected-object format is needed.

Likewise inventory every writer of the retiring prefix: pack/raw, immutable
metadata/finalizer and selected copy effects, not just `_record_sealed_volume`.
Use the same admission interlock or prove the writer cannot overlap terminal
selection. Published collection roots retain their separate deletion owner; an
upload terminal operation must not acquire root disposal rights.

## 7. Responsive execution and resource contract

Keep point status/admission bounded and provider waits in execution. Add a terminal
execution unit owned by `SqlAlchemyCollectionUploadService` and advanced by the
existing archive-maintenance mechanism. Its poll/submit turn is nonblocking: it
claims bounded due work, submits into a bounded service-owned execution pool, and
consumes only finished futures. Do not block `_process_archive_maintenance` under
its shared operation lock on a provider request, join or retry loop. This is a new
work kind in the existing owner, not a second scheduler.

Configure operation/control bytes, per-target/global outstanding calls, prepared
ciphertext buffers, provider request timeout, per-turn SQL/provider page sizes,
reconciliation backoff and retained-intent storage admission. Reserve control and
cleanup capacity before accepting forward payload. An unresolved effect consumes
its slot/storage; exhaustion defers new work without losing original custody.
A timed-out worker still occupies physical concurrency until quiescent. Separate
cleanup capacity allows other constructions to progress while one target is stuck.
If global capacity really is exhausted, report explicit refusal, not silent omission.

Let A be admitted effects actually attempted, R reconciliation/retry attempts, E
required cleanup entries and B exact checkpoint/control bytes processed. Work is
O(A + R + E + B) logical work plus declared index/provider costs. Count failed
calls and uncertain attempts, not only successful commits. Each drain/cleanup turn
is bounded; total number of volumes is not. Point status does not walk all settled
history. Quantum bounds do not imply fixed network latency, constant fsync/unlink
cost, a whole-process memory cap, or a new timing guarantee.

No provider waits inside SQLite/PostgreSQL writers. Per-volume initialization/
completion exclusion preserves bounded parallel part and independent-volume work.
If an implementation chooses a more serial profile, record its throughput effect;
do not hide it behind a small passing correctness test. This reference does not
qualify the existing checkpoint's repeated part-vector serialization growth.

## 8. Public current-only outcome cut

Cancel/discard returns a discriminated operation outcome: HTTP 202 while selected/
draining/reconciling/cleaning; a confirmed original terminal result only after final
commit; a stable conflict/refusal for sealed cancel or changed discard authority.
Status includes bounded phase/reason and opaque continuation, not provider secrets
or an unbounded outstanding-call list. Exact retries across restart/expiry resolve
the retained operation. Missing retained evidence is unknown/not-found, not success.

Update protocol/schema/HTTP clients, both producer forms and maintained CLI/FTP/
derived consumers together. A canceled local wait/connection/producer `stop()` does
not cancel remote execution or release source custody. Heartbeat and explicit resume
retain their existing authority; terminal progress itself cannot extend a writer's
lease. This handoff is a terminal prerequisite to the already designed registration
cut, not permission to reopen P15 planning or weaken its atomic acknowledgement.
