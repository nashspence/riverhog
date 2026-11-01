# Bounded emission with atomic registration publication

Proposed P15 / G10-G11 contract for the exact source named in the handoff.
This document settles a focused owner/state-machine choice; it is not a claim
that the implementation or the larger collection envelope is qualified.

## 1. Decision and preserved meaning

Use the existing collection-upload service and archive-maintenance execution
owner to advance one persisted registration operation in bounded quanta. Store
member, plan and coverage rows in a hidden suffix. After bounded verification,
one small transaction publishes the accepted frontiers, planner checkpoint,
registration counters and immutable acknowledgement together.

A pending operation is NOT registration success, a registered member, payload
custody or permission to release producer state. The success linearization point
is the final marker transaction. Before it, every ordinary observer sees the old
accepted prefix; afterwards it sees the entire new registration, including all
of one artifact's volume plans. SQL rows already committed in staging do not
constitute accepted registration. This deliberately replaces physical one-big-
transaction rollback with equivalent externally atomic publication and bounded
private cleanup. Existing tests must check both visibility and hidden-state recovery.

Do not publish per-volume successes as registration success. Do not use iterator
conversion, ORM flushes, chunked HTTP encoding or a longer timeout to disguise an
unbounded transaction. Do not add an artifact/volume ceiling or another scheduler.
Native, raw/pack plan bytes, archive IDs, conditional object publication, required
history, original custody receipts and archive-root commitments remain unchanged.
Operational request/chain hashes below are not new archive authority.

## 2. Retained owner state and the publication fence

Use current-only tables/columns in the existing server application database,
initialized/upgraded through its StateSchema/Alembic process. No second database,
client-owned server cursor, generic job framework or provider scratch is selected.

The upload retains a published `next_artifact_order` and `next_sequence`, the
published planner checkpoint, and one nullable active registration ID. Explicit
indexed scalar frontier columns must agree with the checkpoint in the same commit;
queries must not repeatedly parse planner JSON or compute MAX across staged rows.

One operation retains:

- Exact canonical ordered request, fingerprint and bounded existing/new partition.
  Include opaque ID, length, SHA, complete raw-part declaration including absence;
  reject duplicates within the batch and conflicting previously accepted metadata.
- Exact construction scope: collection, creation identity, delivery context,
  archive generation/incarnation/storage prefix, pinned policy/checkpoint, and
  current execution/permission references required by the existing owner.
- Base published frontiers and revision; bounded new-member rows; candidate
  planner state; input index; active raw member and next raw offset; next sequence.
- Stage and verify cursors, counts, framed operational chain commitments, expected
  end totals, bounded failure/resource state, execution epoch and due timestamp.
- On success, immutable acknowledgement identity and emitted sequence interval.
  On authorized disposal, a fenced terminal disposition and cleanup cursor.

The server derives the operation ID from a domain-separated hash of construction
identity plus the complete normalized ordered request, and compares the retained
preimage on lookup. It never treats a matching hash with a different preimage as
equivalent. This avoids adding a new caller-generated key that could be lost before
its first response. Authorization is independent of this identifier. The reference
SQL model omits construction scoping and assumes new normalized members; product
integration must implement these checks.

Use the existing artifact order and volume sequence as publication watermarks:
`artifact_order < published_next_order`; `sequence < published_next_sequence`.
Coverage is usable only when BOTH its member and its plan are published. Keep an
operation/generation identifier on staged rows for indexed cleanup and ownership.
Members may be published while in the existing unsealed pending pack; absence of
coverage in that case is not a payload seal.

A single active operation prevents another suffix from jumping over uncommitted
work. Before reuse of an aborted suffix, finish its bounded cleanup. Do not let
old hidden rows become visible under a later larger watermark. Immutable selected
rows are not updated in place by staging. Accepted rows are never deleted as
registration rollback.

## 3. Bounded transition algorithm

### Admit (one bounded request/transaction)

Validate the same registration constraints before reserving work. Pin the current
policy and base planner checkpoint. Recheck current construction access and state.
Read only requested member IDs; previously accepted equal members are read-only
inputs, not appended again. Insert the bounded new-member suffix, operation and
active-operation pointer atomically. Published counters and checkpoint do not move.

An exact pending retry returns the same pending operation. An exact published
retry returns the same immutable acknowledgement, even after later appends or
closure. A different new request while an operation is active returns a typed
bounded busy/conflict result, without queuing it or inserting new metadata.
Ordinary reads and already published work remain available. All-old batches
outside an active operation can produce a bounded no-new-emission acknowledgement;
they must not replay a former planner delta. Same bytes with different IDs stay
distinct. A different ordered request is a different operation, even if it names
the same set of members; existing accepted identities still cannot change.

Admission may accept bounded staging intent while more storage will be needed.
That is explicitly not acceptance of the artifact. Refusal/defer for configured
capacity preserves the original operation for later authorized replenishment.
No already acknowledged registration is turned into a new capacity request.

### Emit one quantum

Use configured maxima for emitted plans, coverage rows, canonical metadata bytes,
input transitions and elapsed execution scheduling quantum. A wall-time target
alone is not a resource bound. Reserve each quantum before its mutation; no
hidden total-volume maximum. A legal single existing-format pack plan must fit
the advertised per-step grant or receive an explicit resource refusal before
selection; do not alter pack membership to fit that grant.

The durable planner cursor resumes at the exact input and raw byte offset. It
must not restart `plan_raw_volumes` from offset zero and skip a growing prefix.
For a raw member of size L and pinned raw-volume size V, emit at offset o:

```
length   = min(V, L - o)
sequence = retained_next_sequence
volume_id = "segment-" + format(sequence, "064x")
next_offset = o + length
```

Construct the unchanged RawVolumePlan and use `raw_volume_plan_bytes`. Increment
the candidate member/byte counters only when that member's full raw extent is
emitted. Existing pack rules are unchanged: flush the prior pending pack before
a raw member; retain small members until the exact original count/byte boundary;
flush once on closure. Zero-byte members follow the existing pack policy. Sequence
representation exhaustion is its existing protected limit, not a new batch limit.

For every quantum, insert plans, exact coverage, cursor advancement and framed
stage commitment in ONE transaction after checking the active operation, base
revision, expected cursor and current execution epoch. On a failed transaction
none advance. A lost commit response is resolved by reading that persisted cursor;
never choose new IDs or replace a conflicting plan. The bounded reference planner
illustrates the raw/pack frontier and uses the maintained format constructors.

### Verify incrementally, then publish

Do not replace the final transaction with a whole staged-generation COUNT, hash,
ORM load or bulk UPDATE. Give verification its own persistent keyset cursor and
one bounded quantum per execution turn. Independently replay the pinned planner
from its verification frontier and compare each expected sequence, canonical plan,
coverage edge, member identity and raw offset/length against the immutable staged
rows. Count/hash the exact framed plan/coverage stream and compare it with the
stage commitment. Check the final end/counters/checkpoint and absence of extra
staged rows with indexed range checks. Missing, duplicated, changed or unrelated
staged rows block publication; retain evidence rather than normalizing them away.

Verification is a second linear pass, not a complete rescan after every quantum.
Successful prefix verification is durable. No writer can mutate a verified staged
row; cancellation changes the operation epoch/state. Arbitrary database corruption
is not solved by a marker: existing storage integrity and downstream plan/content
validation still apply. Any detected inconsistency fails closed.

The final transaction locks/rechecks the upload and operation, live scope and
lease, base checkpoint/revision, ready verification frontier and immutable input.
It updates only the accepted frontiers/checkpoint, bounded counters and bounded
acknowledgement, marks the operation published, and clears the active pointer.
It does NOT enumerate/copy/promote all rows. Uncertain final commit resolves to
one recorded state; reply loss cannot undo or reauthor it. Independent payload
counters must not be overwritten from a stale upload object.

## 4. HTTP and client cut (explicit proposed contract change)

Keep the artifacts POST as the admission operation but return a discriminated
current-only registration outcome. A pending outcome is HTTP 202 with operation
ID, request commitment, bounded phase/frontier and authorized status location.
A published outcome is HTTP 200 with the immutable registration acknowledgement.
The low-level client validates both variants. It must not interpret any 2xx code
alone, or a nonempty member list, as successful registration.

The published acknowledgement contains exact bounded requested member identities,
the original existing/new outcome, construction/operation/request identities and
an immutable half-open emitted sequence interval `[first, next)`. This replaces
`volumes: [all newly emitted summaries]`; it is NOT an empty list pretending no
volumes were planned. If no plans were emitted the interval really is empty.
Mutable payload/custody status is obtained from its existing point-read owner,
not frozen into a replayable registration acknowledgement.

Provide one authenticated operation status GET and one authenticated immutable
volume-summary page GET under the existing upload-session namespace. Names are
integration choices; the semantics are not. Pending pages never expose staged
work. Published pages use `(construction, operation, first, next, after_sequence)`
with the existing authenticated cursor codec and configured count/byte limit.
The original interval stays pinned across later registrations. Continuations
cannot switch construction, request, ordering or bounds. Return explicit next
cursor/exhaustion; no OFFSET, whole-result count or silently dropped tail.
Summaries describe immutable plans, not payload-work permission or custody.

The shared producer retains one bounded pending-batch record in its existing
application-owned durable allocation before the POST: exact ordered normalized
request, construction and request commitment. This complements, rather than
replaces, per-member prepared-primary/source custody; it is not another workflow
registry. A cold restart resumes that batch before forming a different grouping.
Commit the exact published acknowledgement and per-member registration progress
before retiring the pending-batch record; retain the compact operation commitment
needed for uncertain-response reconciliation. Failure to save it is not success.

The SDK's execution path reuses its original prepared members and exact
batch, admits once, polls bounded status with cancellation/backoff/heartbeat, and
returns registration success only on published acknowledgement. It does not hold
a database lock, coordinator slot or unbounded source buffer while waiting.
Reconnect can derive the original operation ID from retained exact request bytes;
regrouping or reordering pending members is not an exact retry. For finalized
readback retain the bounded original acknowledgement with collection-owned
operational state before upload deletion; preserve creator/read authorization.

Change `_needs_upload_scan` to consume the acknowledged nonempty emitted interval,
not the legacy inline list. Then raw-digest submission and prepared-primary replay
proceed. Stop/disconnect does not cancel the admitted operation, release source
custody, or reauthor Native provenance. A canceled local wait returns pending/
uncertain, never success. Update both producers, CLI, FTP, derived writers, transport
schemas/validators, HTTP declarations and maintained interface-parity tests together.
No compatibility alias or parallel old unbounded response is selected.

## 5. Execution, concurrency and visibility obligations

Add a bounded `process_due_registrations` unit to the existing
`riverhog_api.app._process_archive_maintenance` execution path, with lifecycle
state owned by CollectionUploadService. Status GET and repeated admission are
read/point reconciliation, not execution loops. One callback advances at most
one quantum for a selected construction; index due work and rotate it fairly.
Do not loop one large member to completion inside the maintenance coordinator.
Other constructions, finalization, custody and heartbeat must keep progressing.
Resource-blocked operations back off or await a changed grant, not hot-loop.

PostgreSQL: lock in stable upload -> operation order, use indexed bounded due
selection with SKIP LOCKED where appropriate, and compare expected revision/epoch.
SQLite: FOR UPDATE is not an implementation of mutual exclusion. Use a short
write transaction plus conditional revision/cursor updates (or the repository's
backend-equivalent early writer acquisition); retry SQLITE_BUSY as bounded deferred
work. Transaction rollback must discard all ORM state from the failed attempt.
Read multiple controls/rows from one snapshot or pin explicit accepted watermarks;
PostgreSQL READ COMMITTED alone gives a fresh snapshot per statement.

Every control/mutation checks current construction generation and its applicable
claim/authorization. Worker epochs are concurrency fences, not new permission.
Use existing service-owned principal/key/claim lookup, not persisted bearer tokens.
Revocation or replaced execution authority pauses/rejects further mutation and
publication; readback still uses current read access. Never infer live access from
a deterministic operation ID or earlier success.

All paths consuming these tables must use published visibility, including point
member GET, paged/iterated member listings, volume lists/work, raw-digest updates,
provenance binding/history/materialization mutation, upload-unit endpoints,
seal receipt reconciliation, custody candidate queries, payload completeness,
closure/finalization/root/copy/catalog projection, and ORM relationship access.
Hiding rows only in a public GET is insufficient. Cleanup/audit queries that need
hidden rows name the operation scope explicitly. Schema indexes must let normal
work seek the actionable published prefix without scanning the staged suffix.

## 6. Incomplete planning, closure and custody

A new unpublished member accepts neither raw digests nor provenance binding nor
payload work. A direct guessed staged volume ID is rejected by the upload-unit
path, not just omitted from acquire_work. Staged pack rows may contain an intrinsic
readiness flag, but the publication predicate is independently mandatory.

Previously published members/volumes can continue work. Raw readiness still
requires the registered exact part declaration and accepted digest prefix covering
that volume. Publication does not set all raw plans ready. Since raw volume size
is an integral multiple of part size, a bounded digest append makes only a bounded
number of new raw volumes ready; preserve/test that invariant and the index.
A newly flushed pack involving previously registered members becomes usable only
at the current operation's publication, and never releases their sources by itself.

While any registration is pending/verifying/cleaning, `complete` returns an explicit
registration-pending conflict without closing, flushing the tail or recording a
false planner-complete state. Client execution waits for acknowledged registrations
and then retries complete. A complete/admit race serializes on the same upload
fence: either admission wins and closure waits, or closure wins and new admission
fails. Published retries/status still resolve the old outcome without reopening.

`acquire_work.planning_complete` remains false until the accepted planner is closed;
no work right now does not mean planning or payload is complete. Payload sealing
and custody examine only the accepted coverage of a fully emitted raw member.
A first staged/sealed-looking prefix cannot prove complete coverage. Preserve the
existing last-volume receipt/coverage validation and independent raw-content,
Native/history/materialization/completion custody requirements. This design does
not authorize replacing them with counters alone or changing receipt/root identities.

The current last-volume payload-seal verification and finalization readiness may
still traverse a full eligible interval. Their separate declared end-to-end
bounds are not qualified by this handoff. When adapting a touched verification
owner, use bounded persistent verification rather than moving a whole-artifact
scan into the final registration transaction. G11 complete lifecycle proof remains
mandatory; metadata parity is not evidence of verified content or custody.

## 7. Expiry, cancellation, restart and reclamation

Stage work does not renew a producer custody lease by itself. Authorized client
heartbeat/admission follows existing lease rules. Recheck the applicable live
lease/fence on publication and each quantum. Expiry/orphaning retains accepted
custody and hidden staging, pauses new work, and uses the existing authorized
resume/disposition path. It is not permission to discard or take over a live worker.

Cancellation/discard continues to obey existing rules, including refusal where
sealed archive custody forbids cancellation. Do not add a registration-abort
endpoint with broader rights. An authorized terminal transition first fences the
operation under the upload lock. No stale worker can publish afterwards. Quiesce
workers that can still mutate old published payload state before its owner cleanup.
Staging itself makes no provider calls and creates no provider object to roll back.

Clean hidden coverage/plan rows by operation and increasing sequence in bounded
transactions, then bounded staged-member/operation controls; delete child edges
before FK parents. Do not invoke a collection-sized cascade as a final small step.
Keep the disposition and reservation until exact cleanup completes. Lost cleanup
responses repeat idempotently without deleting accepted rows or double-releasing
capacity. Original frontiers stay unchanged. No suffix reuse before cleanup ends.
Published operations/acknowledgements follow collection-owned operational retention;
TTL, connection loss and reference retirement are not disposal authority.

## 8. Bounds and proof obligations

Let M be the admitted request-member maximum; K the existing pack-member policy;
V emitted volumes; E coverage edges; D serialized metadata bytes; Z database size.
A quantum admits plan count b, coverage-row count e, serialized bytes d and input
transitions t. Stage/verify/cleanup are bounded by those grants plus the declared
single-volume construction and bounded control cost. Memory is independent of
V: request/pending-pack state plus one bounded emission/verification window.
The existing pack author and checkpoint representation still cost O(K); they are
not constant memory and must be charged. The scalar raw frontier does not retain
or reserialize an ever-growing emitted-plan list.

Successful stage + verification + cleanup totals O(M+V+E+D), plus necessary pinned
pending-pack/control processing, backend indexed seeks I(...,Z), and explicitly
counted retries. Each volume is generated/checked a fixed number of times, not
regenerated from zero for every continuation. A failed attempt pays at most its
quantum and retry checks. Keyset pages read only the requested interval. Publication
is O(M + bounded checkpoint/ack metadata), independent of V and E. Backend B-tree,
lock and durability costs are measured separately, not called O(1).

Staging needs O(M+V+E+D) durable metadata until publication/cleanup. If physical
capacity runs out, preserve frontier and exact intent, report/defer, and permit
operator replenishment or authorized disposition. This is resource admission,
not a hidden support ceiling or successful preservation of a partial artifact.
Keep enough control/cleanup progress capacity under existing operator policy;
do not reserve the entire hypothetical archive in RAM or use custody to evict it.
Existing Numeric/sequence representation bounds remain explicit contracts.
