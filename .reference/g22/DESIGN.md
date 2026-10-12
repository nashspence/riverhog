# G22: exact-prefix validation in bounded, recoverable steps

Recommended replacement design for #964 under #903. Audited source:
`152d095038df0c0d0ed642cf265d47b11c65a754`, tree
`991e994457b487a7dca016b7fc0409485cef25c6`. Reference parent:
`4621be70fe2229ef7de169862ff1ec5274dec00f`.

Earlier recommendations are calibration, not a requirement to preserve unsuitable
execution. This document supersedes the former monolithic epoch transactions,
per-attempt graph-sized savepoints, crash-restart-from-zero default and instruction
to apply integration.patch as the current implementation. The correctness basis
and honest cost profiles are retained in [ALGORITHMS.md](ALGORITHMS.md).
This is a design submission, not a production patch or conformance declaration.

## 1. Decision and acceptance boundaries

Keep certificate-first replay for supplied complete finite journals and the
level/fuel online algorithm for genuine online operations and invalid-epoch
replay. Execute BOTH as persisted state machines with bounded loads, computation,
writes, checkpoints and cleanup. A semantic frame, correction, graph insertion,
epoch or whole journal is NOT automatically a physical transaction.

The public distinction is unchanged: transport append acknowledges exact octets;
semantic sealing and validate_journal_chunks accept only after all required
prefixes, profiles, identities, provider fences and tail checks succeed. Never
accept a bad prefix because a later correction repairs the final graph. Speculative
ranks cannot supply future objects, references, recorder authority or metadata.
A genuinely online call still returns accepted/rejected at its existing boundary;
internal suspension is neither provisional acceptance nor a later rejection point.

Bounded steps do not promise constant-time validation of arbitrary schema programs
or online graph operations. Large operations can require many steps and more total
work. Do not relabel capacity failure, cancellation or unavailable input as semantic
invalidity. Preserve native validation/error order on comparable completed inputs;
retain earlier semantic failures ahead of later speculative parse/graph findings.
No graph/cardinality/depth ceiling is added to make an operation fit one step.

## 2. Owning APIs and checkpoint trust

In riverhog-provenance, add a storage-neutral JournalValidationSession with
`advance(budget) -> progress`, `result() -> JournalSummary`, `close()` and explicit
attempt identity. The current synchronous validate_journal_chunks wrapper drives
this session to completion and keeps its existing result meaning. The core library
has no Riverhog catalog, HTTP, credential or archive-release dependency.

Riverhog's collection_uploads validation owner binds a resumable session to its
already retained exact uploaded journal, expected root/bytes/anchor, contract/profile
pins, validation mode and existing job epoch. Archive-only recovery can use the
same neutral engine with its own authenticated source and local workspace.
A nonrepeatable caller's stream cannot magically resume across a process death:
recovery requires an owned durable capture or an exact replay-capable provider.
Never claim a library iterator alone supplies durable source custody.

Suggested owner-local state, not a universal job API:

- validation_attempt: attempt/source/algorithm/schema identities, worker epoch,
  phase, version, active epoch/frame/operation IDs, accepted-prefix marker,
  durable source offset, terminal state and fixed-shape counters;
- spool_segment: immutable segment ordinal, exact octets/length/digest and source
  binding; frame_directory maps exact entry positions to segments;
- epoch_work: bounded scan cursors, separate graph-kind counts, active vertex and
  adjacency cursor; frame, assertion, object, arc, degree, ready and rank relations
  are attempt/epoch-qualified;
- operation_work/undo: phase and indexed first-before-image records for touched
  rows; no serialized graph, complete visited-ID list or whole context in a row;
- semantic tables: native exact-prefix identities, active/retracted facts, references,
  lifecycle extrema, findings and frame records. In-flight state is quarantined.

Headers have fixed shape. Large sets, deep parser/traversal stacks, variable values
and undo records use separately paged relations. Counters/ordinals use their actual
owning scalar domains; decimal text requires numeric ordering, not lexical ordering.
No diagnostic SQLite integer width is silently made a public total limit.

Checkpoints are execution evidence, never archive or validation authority. Restore
only server/owner-created durable state bound to the exact source, profile catalog,
algorithm, epoch and accepted-prefix marker. Verify storage integrity and manifest
links before use; a client-supplied pickle, SQLite file or hash context is not an
admissible shortcut. An ordinary worker/process restart must not imply source loss.
Corrupt/lost durable storage is an explicit failed/rebuild disposition, outside the
bounded ordinary-restart promise, not an automatic success or zero-cost cache miss.

## 3. Work-unit protocol and durability

Each transition loads at most the row and byte budgets using indexed cursors,
closes source/database resources, computes on detached data, and commits bounded
writes plus the exact next checkpoint with epoch/version compare-and-swap. A lost
worker cannot advance or clean up a replacement attempt. Quotas must reserve the
actual write, undo and segment space before admission; suspension retains completed
work where the owning lease permits. Include schema/hash CPU delegates and lock
waits in observation. Cancellation and deadlines are checked between bounded units.
A wall deadline alone is not evidence that the SQL plan or loop is bounded.

Private durable state uses crash-safe transactions and synchronized segment
publication. The historical scratch `PRAGMA synchronous=OFF` is NOT a resumable
checkpoint store. Write and synchronize each bounded file segment before atomically
publishing its DB reference/checkpoint; a pre-commit orphan is reclaimed in bounded
cleanup. References to missing or changed segments fail closed. Reopening validates
source/algorithm bindings and progress, not a full graph rescan.

SQLite private state and the Riverhog catalog cannot share an atomic transaction.
Commit neutral progress first; then publish its token in the catalog under the
current job fence. Recovery may discover newer neutral progress only for the SAME
owned attempt; catalog completion is not inferred from a file's existence. A
process-lifetime scratch lease cannot be presented as durable job retention. Keep
resumable files in the owning job workspace across worker replacement; process temp
files remain disposable and separately budgeted. Test both kill and power-loss
failure models when claiming either; diagnostic exception rollback is not fsync proof.

## 4. Explicit bounded phases

CAPTURE reads bounded exact source ranges into retained segments, carrying a framing
cursor across segments. Existing entry-byte limits remain genuine contract bounds;
removing G26 posting limits does not enlarge G22's entry contract. A permitted frame
may be decoded as an entry-bounded atom outside a DB transaction, with its schema/
canonicalization cost declared separately. Any permitted larger profile requires
its own streaming/parser design, not a hidden new rejection. Checkpoint after a
bounded frame or finer parser unit; at most that admitted atom may need replay.

EPOCH_SCAN identifies correction boundaries and captures the topology union, but
records no accepted semantics. Store original frame offsets, not a second complete
copy of every cumulative prefix. Seed carried active edges once for the real epoch
using a keyset cursor. For each unique arc, update endpoint indegrees only when
that arc first enters the epoch union; duplicate documentary assertions retain
separate identity/multiplicity. Correction deletions affect the speculative active
set at the start of its new epoch, while native replay still validates their legality.

RANK_SEED visits the vertex directory in bounded pages and enqueues zero-degree
vertices. RANK_POP removes one indexed ready vertex. RANK_EDGES keeps an active
vertex AND the last outgoing edge key; it processes at most the remaining edge
budget, updates degrees, and commits its cursor. A million outgoing edges do not
fit one pop. Track declared/settled counts transactionally; no terminal COUNT scan
or whole-epoch GROUP BY/INSERT SELECT is hidden inside a small-looking call.
A cyclic union disables its graph certificate; native replay determines the actual
earlier error. Rank tables never validate the remainder of the journal by themselves.

REPLAY executes native checks in original order. Small entry-local checks may be
bounded by the existing entry envelope; graph/correction/reference loops must keep
explicit suboperation cursors. The frame's accepted-prefix marker advances only
after every subphase succeeds. Source bytes, SHA state, predecessor reference and
semantic watermark advance together. A crash may leave an in-flight private
operation but cannot expose it as an accepted prefix.

CORRECTION first verifies every exact retraction reference and uniqueness against
the previous accepted prefix. It then retires those assertions and rebuilds the
necessary effective-view aggregates in bounded stages. Retraction lists and active
rows are paged. Old aggregates are epoch-qualified and incrementally retired, not
cleared by one graph-sized DELETE. Preserve native within-correction semantics;
do not validate or expose arbitrary partial subsets as separate corrections.
The cost still includes the active view rebuilt for each real correction.

ONLINE uses the selected level/fuel logic with persisted backward frontier, seen
set, fuel, forward queue and per-vertex adjacency cursor. Clearing a promoted
vertex's horizontal incoming set is paged or generation-qualified with accounted
cleanup. One accepted edge may require graph-sized TOTAL work, but not one
transaction or one unsuspendable adjacency walk. Avoid one complete state clone
per insertion: log the first old value of each actually touched row. On failure,
undo those rows in bounded steps before returning a reusable rejected checker.
The checker is quarantined while mutation/undo is incomplete. No observer sees a
half-repaired online graph. No next operation is admitted until the previous one
has committed or rollback has completed. Success advances a marker; reclaim undo
entries incrementally. A future overlay implementation needs its own lookup-cost
proof; an ever-growing chain of overlays is not the selected default.

TERMINAL verifies exact expected anchors/tail, complete provider fence, zero pending
obligations and bounded terminal metadata; freeze only then. Normal cleanup removes
retired epochs, undo entries and source/graph rows in bounded indexed batches.
Directory unlink, VACUUM and TRUNCATE are not assumed constant-time. A hard-killed
attempt is fenced and reclaimed by its owner without disturbing a later attempt.

## 5. Exact SHA-256 continuation, not a different journal hash

The native validator hashes `frame.encoded` and compares exact prefix SHA-256 values
at checkpoint/anchor boundaries. Do not replace those values with a chunk tree,
hash chain or digest of digests. Hashlib.copy() is not a portable durable-state API.

Select a reviewed SHA-256 backend exposing a private versioned continuation codec:
eight chaining words, processed-byte position and at most 63 residual bytes, plus
algorithm/state-format identity. Resume at the exact original octet offset. Keep
accepted-frame offset distinct from partially hashed/captured offset. Finalization
pads a COPY of the state; computing an intermediate prefix digest cannot alter the
continuation. Bind state and cursor in the same durable checkpoint as the associated
semantic/source progress. Range-check and authenticate/trust-bind restore through
the owner; shape checks alone do not prove the state was computed from that source.

`../g22_g26/sha256_state.py` is an independently written diagnostic engine checked
against native hashlib, not a production backend selection or crypto approval.
Its RFC message-domain/fixture codec limits must not become a new narrower public
journal limit. Reconcile the actual native scalar/hash representational envelope at
the owner. Do not pickle OpenSSL internals or invent portable serialization for an
opaque hash object. An implementation lacking a reviewed continuation backend cannot
claim bounded durable restart for this path; restarting the whole journal is not
the approved default workaround. Backend cost and additional review are explicit.

## 6. Explicit resource ownership and downstream consumption

JournalStore already has close(); the problem is enforcing its use across summary,
reader, cache and rebuild lifetimes. Make the current JournalSummary closeable and
context-manageable through its owning frozen store, without changing semantic fields
or digests. Define owned versus borrowed readers; closing one borrow cannot delete
a store still used by another owner. Closed views fail explicitly. Finalizers are
a safety net, not the main policy. Include generator early-exit and Windows tests.

G26 consumes a retained source-fact directory or explicit bounded summary handle,
not a dictionary of one live complete summary/connection per snapshot/member.
After publishing the source's exact facts and checkpoint, release the handle; reopen
only the exact retained source if needed. Full graph/materialize/states convenience
results remain materializing operations with output-sized cost, never called inside
a claimed bounded-source traversal. New close/session surfaces get normal exact
Python-contract ownership and extent/growth evidence; they are not exempt as internal.

## 7. Complexity, implementation and acceptance

Keep the T0, T1, O and M profiles in ALGORITHMS.md. Bounded scheduling changes neither
the correction sum nor the online amortized proof. Charge row/key bytes, B-tree work,
undo traffic, durable writes, input passes, crypto/schema costs and cleanup. With
retained durable state, F ordinary crashes add at most F unfinished work units (or
explicit bounded entry-local replay atoms), plus bounded recovery/index lookups.
State loss, source replacement, unlimited failures and insufficient capacity have
separate explicit outcomes. Do not manufacture corrections or accepted snapshots
to rationalize avoidable repeated work.

Integration order: (1) owner-reviewed session/checkpoint and hash codec;
(2) journal.py stateful framing/prefix/tail and deterministic close;
(3) journal_store.py checkpointed checks/undo/epoch aggregates;
(4) bounded epoch and online kernels using the retained algorithm witnesses;
(5) collection_uploads job retention/fences and archive/recovery adapters;
(6) complete native parity, work-count, kill/restart and ordinary qualification.
Do not blindly apply the historical integration.patch. Its algorithms remain useful,
but it does not implement this durability or transaction protocol.

Required witnesses include high-degree and deep graphs; correction storms and legal
edge reversal; invalid prefix later repaired; earlier non-graph error in a cyclic
epoch; missing/late source and profile failures; duplicate IDs/parallel assertions;
large touched-row undo; failpoints in every phase; state/cursor mismatch and stale
workers; original SHA at every boundary; fair progress and actual connection release;
full independent recovery with no server dependency. Final locking/SQLite/Windows,
locked gates, built artifacts and exact-SHA CI remain integration obligations.
See ../g22_g26/README.md for diagnostics actually run and their deliberate limits.
