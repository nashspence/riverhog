# Honest asynchronous retirement scopes

This is the focused #989 source/declaration/witness proposal at
`dd09f4ab5e824935e680f26fba3d73d2484cb426`. It leaves archive authority,
original-resource receiving, ordinary/terminal budgets and runtime activation
unchanged. The candidate is explicit material under `.reference/`, not product
integration. Apply only in a disposable worktree after reconciling current source.

## 1. Decision: bookkeeping is not completed retirement

For the existing `advance(limit=L, octet_budget=B)` successful return, use the
name **bookkeeping events**, with a worst-case upper bound `4*max(0,L)`.
Do not retain the obsolete meaning of `progressed_units` as confirmed file,
catalog or blocker progress. There is no numerical timing target.

The proof is from independent source loops, not from the maximum sample:

| Phase | Actual counted events | Bound |
| --- | --- | --- |
| Registry collection | Each exact joined physical outcome received; each original ordinary reservation successfully withdrawn | `C <= L`, `W <= L`, so `C+W <= 2L` |
| Original-close resumption | Successful submissions of close-only work for retained originals | `S <= L` |
| New admissions | At most one successful submission in each outer iteration | `N <= L` |
| Return | `C+W+S+N` | `<= 4L` |

The collection visit and withdrawal are deliberately separate. A 17-byte file,
three-byte quantum and `L=1` can return 1, then 3 after the first original has
joined and positively received: one completion + one withdrawal + one new
submission. A different retained close-only original makes all four events
possible in one call. Neither value proves three/four bytes, files or successful
retirements. The worker's exact outcome and primitive evidence answer those
questions. Return zero is not evidence that no worker remains active.

Validation of caller controls stays before effects. Nonpositive integer limits
return zero under the existing contract; invalid boolean/noninteger limits or
nonpositive byte grants still reject. A raising call has no returned counter;
its already performed child work remains in the partial execution evidence.

Three candidate tries per outer iteration remain independent: at most `3L`
select/membership/submit attempts, at most two excluded identities per select,
and at most `6L` copied exclusion references. Capacity, a retained original, or
busy custody may make fewer admissions. No larger archive or workspace ceiling
is implied by these per-call limits.

## 2. Source-owned scope graph

The candidate uses the existing `Declaration`, typed dimensions, monotone numeric
bound envelopes, `declare_growth`, `project_bindings` and
`validate_execution_samples`. It adds no evidence registry or framework.

**Scheduling root: `UploadJournalValidationRetirement.advance`.** Actual children
are `_select`, registry `collect`, `resume_ready`, `retained` and `submit`.
The registry children account for exact scope/control serialization and hashing,
ordinary retention and the existing physical submission/collection owners.
Physical collection's zero-wait join/liveness primitives remain included.
No worker `_advance`, blocker formatter or resource close is an inline child.

**Original physical callback: `JournalRetirementRegistry._run`.** It records
passive original actor construction followed by one forward quantum, OR a
close-only invocation of the already-retained actor. The forward quantum contains
actual `_advance` catalog/filesystem work, original receiving and, on the existing
capacity/custody error path, complete error formatting and the real `_blocked`
conditional mutation. The unchanged unit/catalog declarations and primitive
observers supply their detailed costs. The required original `turn` is passed.

**Receiving: `JournalRetirementTurn._receive` and `close`.** These contain the
original file receiver and reverse-dependent namespace callbacks. Independent
input counts are the actually retained callbacks and native resources before
entry. Duplicate references to the same resource count once. Native metrics
measure attempted close/open entries; there is no second open on receiving.
Close-only work cannot call `_advance`, truncate again, reconstruct an actor or
release another original's grant. A callback that refuses remains installed.

**Exceptional receiving** is a separately declared graph traversal on execution.
For V distinct exceptions, E cause/context/group edges and T traceback frames,
there are at most `1+E` work-list visits, V clear calls and T traversed frames.
These are independently determined inputs; a complete string or graph is not
bounded merely because the detached blocker stores 1,000 characters. Full
formatting octets are charged before slicing. Existing valid UTF-8 text controls
retain their current semantics; rejection is not successful blocker publication.

The registry's own ordered-map work is explicitly measured. Collection uses one
lookup per returned completion, at most three map mutations per rotated context,
and one length query: `<=4L+1` entries. Resumption uses `<=2L+1`; new submission
uses at most a lookup and an insertion; membership uses one lookup. Validated
workspace keys are 32 ASCII characters. No catalog/context census is permitted.
Hash-table internals, OS/index lookup costs and callback latency retain their
existing backend terms; entry counts are not a wall-time guarantee.

## 3. Attribution across a scheduling return

An asynchronous submission establishes a causal link, not a synchronous delegate
scope. A worker may start before the scheduler returns, or much later. Thread-
local observation stacks keep its primitives out of the caller's root in either
case. Do not extend the scheduling sample's end timestamp through worker join.

Evidence uses one case-local original-lifetime token assigned by retained object
identity, the actual physical worker object/attempt, exact reservation/scope and
forward-versus-close kind. A new quantum after original receipt has a new lifetime
token even when the job/version/scope hash happens to be unchanged. A close retry
keeps the original token and actor, but has a new physical attempt. Bare numeric
thread IDs are not durable evidence identities: they can be recycled.

The witness retains separate roots and an ordered causal transcript: admission,
worker entry, scheduling return, worker root result, exact original join,
physical outcome receiving and ordinary grant withdrawal. The transcript is
local test evidence, not another production state registry. Every submitted
attempt must have one original worker record and one actual join/retained-pending
disposition before the test ends. Missing, duplicate or cross-original attribution
fails the finite transcript checker; a positive sum without those links is insufficient.
Existing originals at the boundary are explicitly marked carry-in. Their earlier
work is not fabricated as a new submission; outstanding joins are recorded only
from the exact retained worker. Unknown/pending originals remain visible.

For a complete observed interval, charge the sum of scheduling roots plus the
sum of worker roots and separately observed shutdown/coordination work. Within a
root, primitive totals include actual child invocations once; nested parents are
not added again as new physical work. Retried and failed attempts remain visible.
An unfinished interval is explicitly pending, not reported as complete retirement.

## 4. Keep diagnostic frame receiving off coordination

The audited `collect` explicitly clears exception frames. Python frame clearing
releases local references; it is not a constant-cost bool check, nor a file-close
receipt. Merely declaring the worker graph while keeping that cleanup on the
coordinator would hide work after the physical result was joined.

The candidate therefore retains the first failed outcome with its original
context, and performs its frame receiving on the original close-only execution
path after positive actor receiving. It transports a small internal pending
marker for a failed close attempt; it never treats that marker as `_Closed`,
file progress or permission to retire ordinary capacity. Secondary close errors
are received on that worker as well, instead of being dropped by the next poll.

The same context retains a secondary diagnostic before disposal. Failure of
that disposal keeps the diagnostic and original charge for a later execution
attempt. The first original failure/outcome is not replaced by a reconstructed
actor. This adds bounded ownership fields to the existing context, not a queue of
all failed attempts. Unknown descriptor/Native/SQLite outcomes still follow
#987/#988's fail-closed original owners; this helper cannot certify them.

The selected Native actor creates no new resource during a close retry: every
file, namespace and callback obligation is already retained on it. Frame cleanup
cannot substitute for receiving those owners. Any callback violating that
pre-retention contract is still a blocker, not made safe by exception formatting.
Arbitrary finalizers are not bounded by a traceback-entry count. Their execution
belongs on execution and with their actual owner; no hard aggregate-memory or
arbitrary callback-latency guarantee is claimed by this packet.

The existing coordinator warning records only exact workspace identity and the
exception type. Its configured logging sink remains a separate coordination
assumption and delegate cost: it must not perform an unbounded blocking export
inside this path. The proposal does not add a logging scheduler or pretend a
method-call count proves sink latency.

## 5. Custody, capacity, shutdown and liveness

Keep these predicates distinct:

1. The exact original actor positively reports its file/namespace/Native/SQLite
   resources received under their own contracts.
2. The actual physical thread for that attempt has exited and is joined by its
   existing execution owner, not inferred from `Future.done()`.
3. The exact ordinary reservation is withdrawn by its retention owner.
4. Semantic catalog retirement is justified by its existing durable phase/fences.

The pool may release a terminal physical slot after actual join while an ordinary
reservation remains held for unresolved resource receiving. A later receiving
attempt consumes a fresh terminal execution grant but reuses that original actor.
Independent admitted work can proceed on another target/workspace. No expired
retry timer, completed Future, joined worker or detached bool force-receives an
unknown resource. Runtime activation remains off.

`close()` only stops new admissions. `receive_stopped()` is an explicit shutdown
execution phase after joins; it can visit retained originals and wait in receiving.
It is not covered by the scheduling `4L` bound and must not be invoked from a
status/poll path to evade that bound. Its diagnostics and original reservations
must also receive before shutdown reports settled.

## 6. Concrete consumption

`prepare.py` is an exact-source, opt-in candidate overlay. It changes the owner-local
scope bindings, extracts blocker formatting and exception graph receiving,
adapts the shared original-close/collection boundary, and supplies the maintained
loop/edge observations. The published product tree is unchanged; the workflow
records hashes of the explicitly overlaid source. Routine module/style choices
remain integration's responsibility after source reconciliation.

The maintained service-scheduler declaration derives its retirement child bounds
from the source binding. Its outer return still counts truthy loop activity once,
not completed workspace retirement. Preserve that distinction when integrating;
do not reintroduce the old inline child graph at the service layer.

Start with the 17-byte original and the three/four bookkeeping witnesses. Then
run the preserved matrix and real busy/unknown receiving tests, followed by the
required exact-final-source/tested-merge gates. The independent-arm 1,000-member
proof and retained 128-file lifecycle remain the owning #964 campaign. This
reference is neither that campaign nor permission to activate or release.
