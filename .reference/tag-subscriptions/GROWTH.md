# Extent and growth supplement

External reference input for the owning issue, not a second production contract
registry. Prepared against `main` **e0036f80410a83cc5eaa9d63c17650ffae72e2bd**.
The issue's dated supplement controls recommended scope; declarations and tests
must ultimately live on the actual operation owners. This supplements and, where
explicitly stated below, narrows the original reconciliation/completion wording.
It does not change the destination-oriented hard cut or introduce another legacy
materializer, global cache, generic scheduler, or independent tag engine.

## 1. What the current sources establish

The current `AGENTS.md` distinguishes extents, enforced growth bounds and measured
performance targets. `scripts/extent_contract.py` owns extent principles and
source/schema associations. `riverhog-growth-contracts` attaches dimensionally
checked `Declaration`/`Bound`/`Expression` data to exact operation bodies;
`scripts/growth_contract.py` discovers them, and `scripts/validation_witnesses.py`
binds actual assertions and primitive observations. A declaration, candidate test
association, profiler run or small passing model is not production proof.

The current local materializer accumulates complete inventories in `_inventory`,
walks existing provenance in `_ensure_provenance`, and repeatedly scans/hashes
members in `_missing_artifacts` from `_sync`. Preserve its integrity obligations,
not those traversal costs in each scheduled invocation. The shared spooled
planner is useful but its indexes/sorts and provenance closure processing still
need their own cost accounting. A generator does not prove bounded memory if its
consumer materializes everything or repeats the generator for each member.

`CatalogFollower` has bounded validated pages, generation-independent opaque
positions and no internal retry loop. Its public batch still omits `caught_up`.
The server's `changes` method scans a bounded slice of catalog revisions **before
visibility projection** and binds a fixed horizon in the cursor. Empty returned
pages can require real work and advance that horizon. Count pages as well as
visible observations; do not claim cost proportional only to matching collections.

`ApiClient.stream_retrieval_artifact` already accepts exact byte ranges and checks
ETag, Content-Length and Content-Range. A partial range is NOT a whole-file SHA-256
verification. `CheckpointSHA256` supplies exact resumable hash machinery; its
serialized state is operational state, not independent proof of scratch bytes.
Current frozen journal/capture and member-history code must be reused at their
public ownership boundary rather than rewalking every journal for every member.

Pinned entrypoints are listed in the integration map. SQLite's primary guidance
also matters: a LIMIT alone does not prevent scanning/sorting; parent deletion
can do arbitrary subordinate work through cascades; deleted pages can stay in a
file for reuse. See [query planning](https://www.sqlite.org/queryplanner.html),
[foreign keys](https://www.sqlite.org/foreignkeys.html),
[PRAGMA](https://www.sqlite.org/pragma.html), and
[progress counters](https://www.sqlite.org/c3ref/progress_handler.html).

## 2. Scopes and independent dimensions

Declare separate scopes: one public API page; one accepted catalog proposal; one
indexed work selection; one materialization metadata/byte turn; one explicit audit
slice; one cleanup slice; one complete no-failure materialization; one finite run;
and the retained root/host-state footprint. State where each starts and ends,
what caller preparation is outside it, and how its delegates compose. The whole
operation must account for excluded preparation at its actual owner.

Use independently counted dimensions, not quantities manufactured by the measured
algorithm to excuse its own repeated work:

- `P`: catalog pages fetched, including empty advancing/idle pages; `U`: descriptors
  and changes received; `T`: explicitly selected tags (one for the CLI).
- `C`: distinct source/collection identities ever observed by this root; `A`:
  admitted collection instances, including suppressed ones; `K`: relevant indexed
  state size. `C` can be much larger than this tag's matches.
- `M`: necessary member/placement/edge metadata; `J`: unique required provenance
  input octets; `B`: selected payload octets; `O`: actual prescribed output octets,
  including repeated primary-prefix sidecars required by the content contract.
- `R`: injected/observed interrupted attempts; `W`: bounded replay/revalidation
  extent for each interruption; `D`: independently expected metadata bytes.
- `p`, `q`, `w`, `c`, `g`: effective page, byte-turn, transfer-window, concurrency
  and cleanup-unit budgets; `H`: retained presentation history bytes/rows; `Q`:
  live pending work; `X`: retained partial-output bytes; `S`: installed schedules.

Carrier limits and resource policy cannot change semantic identities, drop an
obligation, reinterpret a valid large collection as malformed or claim completion.
A finite namespace can grow without a product total cap while its step RAM and
work remain bounded. No finite disk and no overloaded worker can promise eventual
completion for arbitrary arrivals. State stability/liveness conditions explicitly.

## 3. Incremental reconciliation is not an implicit full audit

A true idle invocation means the feed is caught up with no new source revisions,
no due work, no requested audit and no due cleanup. It performs one catalog poll,
a fixed set of indexed state checks/counter reads and one bounded summary update.
It performs **zero member-payload or retained-provenance reads/hashes**, no
collection enumeration and no per-completed-collection remote authorization loop.
Its local indexed lookup cost may be logarithmic in K; do not promise literal
constant SQLite disk I/O. Schema/header verification can depend on the fixed
schema size, not on materialized payload. SQLite recovery after a crash is an
explicit exceptional scope, not silently part of an O(1) healthy-idle claim.

An empty advancing catalog page is not idle: count its request, scanned server
slice at that server owner and accepted continuation. A finite run stops on the
validated caught-up flag or an exhausted budget, retains its continuation and
does not chase a new horizon. No fresh tag or timestamp-based watermark replaces
opaque cursor authority. Repeated idle runs cost their polls/summaries, not
`runs * retained_payload`.

`current`/`converged` now explicitly mean the selected catalog horizon and admitted
nonsuppressed obligations are settled **according to verified publication
receipts and known attention state**. Return `integrity_basis=verified-receipts`,
last complete audit identity/time (or null), and the audit scope actually checked
this run. Do not say that all local bytes were checked, are currently readable or
remain unmodified. Known missing/modified/unverified bytes still block convergence;
unknown out-of-band changes cannot be detected without checking them.

Keep explicit audit and repair. An audit is a restartable scan over a captured
manifest generation/upper key, with an independent member/byte budget, persisted
frontier and exact final scope. It eventually examines all selected bytes in a
quiescent finite root but does not restart at the first file every invocation.
An ordinary reconciliation run does not silently schedule a whole-root scrub.
Fresh authorization remains required before actual remote effects; a receipt is
not permission to download. Rebaseline cannot silently authorize old pending work.

## 4. Operation obligations

| Operation | Required cost and extent |
| --- | --- |
| Catalog follow + exact predicate | Client calls bounded by `P + U*T`, plus separately counted retries; metadata bytes by actual bounded carriers. No whole tag-set hydration to answer one membership question. Server tree/visibility costs remain declared delegates, not assumed O(1). |
| Proposal acceptance | O(U) constant-width state mutations and indexed seeks; declare B-tree comparison/page costs separately, typically O(U log K). Cursor, observations, admitted work, counters and path reservations commit together. |
| Due selection | Use an index matching readiness/due ordering and exact tie breakers; O(log K + returned candidates) for a covering seek/scan, with any row fetches separately charged. No scan of completed/blocked/future work, OFFSET or full count per turn. |
| Materialization | Traversal/hash/copy work linear in necessary B, J, M, D and O. Shared provenance is verified at its exact reusable scope, not reparsed per artifact. Account for actual sidecar output multiplicity: O can exceed unique input bytes. Spool/index/sort costs are declared and justified, not relabeled linear. |
| Placement | Bounded template expansion plus exact indexed reservations/direct path checks. At most bare-name and full-disambiguator candidates; no probing `-2`, `-3`, directory listing, sibling sort or renaming previous winners. |
| Status/list/history | Summary counters maintained with mutations. Single-root status is bounded; ordinary page traversal uses seek keys and fixed carriers. Literal filtering may need a budgeted candidate scan and return an empty advancing page. Exact filtered totals or unindexed sorting require an explicit restartable query scan, not a hidden full scan behind `--page-size`. |
| Audit/cleanup | Persist traversal position; each row, edge, file and parent removal is charged. No unbounded CASCADE, DELETE of an entire generation, rmtree, VACUUM or hash-all inside one budget unit. |
| Scheduler | Single-job inspect/invocation is independent of unrelated roots. Listing jobs and pruning host diagnostics are paginated/budgeted. Missed ticks coalesce; no job per missed interval. End-to-end cost across independent roots is the sum of their catalog work, not work only for distinct server collections. |

In a no-failure materialization, required bytes should pass through each necessary
hash/copy/verification stage a fixed, explained number of times. Validate actual
constants at the production owner. A page bound alone permits quadratic prefix
rescans; memory bounded by a spool alone permits repeated whole-resource scans.

Report retry overhead separately. Expected cumulative primitive work is base work
plus bounded replay/revalidation for R interruptions; there is no failure-independent
completion promise under infinitely many interruptions. Do not put avoidable
repeated work in a dimension named `necessary metadata`. Loss of custody, a changed
source, expired capabilities or corrupted state can require explicit revalidation
or reset; they never permit bypassing integrity to satisfy a complexity target.

## 5. Finite turns, byte continuation and fair service

Keep `--max-steps=100` as the default number of control turns, not a time guarantee
or a license to perform a whole arbitrary file/collection inside a control step.
A declared turn is one bounded catalog proposal, one metadata page/segment, one
due-work selection/transition or one cleanup slice. A catalog page with p upserts
may perform up to p*T exact membership calls; charge that fan-out explicitly.
Use existing protocol page/body maxima and early streaming decode bounds.

Add explicit byte-turn and run byte budgets to the application-owned execution
settings: recommended initial defaults are **16 MiB per range/verification turn,
256 MiB transferred and 256 MiB locally verified per run**, each positive and
operator configurable. These defaults apply to `tag-subscription reconcile`, whether invoked manually
or by a schedule. The one-collection `collection materialize` command retains its
completion-or-pending behavior for that finite selection using bounded internal
turns; it does not unexpectedly stop at 256 MiB unless the operator explicitly
requests a finite byte budget. Declare its whole-operation byte extent separately.
These limits bound execution, not file size. Count retries
against the same run budgets. Reserve the shared allowance before dispatch so c
workers cannot each consume the full run limit. Preserve the maintained download
concurrency/window defaults and their owner; do not duplicate their definitions.

Consume each range completely before leaving the maintained client context. Bind
durable offset/hash continuation to exact root/artifact/length, retained attempt,
private scratch identity and accepted bytes. Flush owned bytes before checkpoint
advancement; on retry retain committed prefixes and reconcile only owned surplus.
Never serialize Python hashlib internals ad hoc. Reuse the exact checkpoint
primitive or an existing authenticated-chunk/checkpoint owner. Require complete
expected whole-file SHA-256 verification before publication. Range ETags alone
and valid checkpoint syntax do not establish that the assembled local file is
correct. A required final full verification pass is itself resumable and accounted
for; it must not become a prefix rehash on every range turn. Revalidate/refuse
when custody assumptions no longer hold.

Metadata workers, lease maintenance, signal/cancellation handling, progress and
data-transfer workers have independent bounded lanes. Persist a rotating lane
cursor and fair per-intent turns so `max-steps=1` across many invocations cannot
always choose catalog work first. Retry backoff must produce a future due time;
blocked/over-capacity work cannot stay at the ready queue's head. Cold readiness
polling occupies a control turn, not a waiting transfer slot. A continuously
eligible finite set is serviced under positive budgets and repeated invocations;
no unconditional throughput/deadline claim under overload or permanent failure.

A byte quantum is not a strict wall-clock bound on HTTP/OS I/O; retain supported
timeouts/cancellation and expose pending/interrupt truth. Full completion of a
large valid file may span many ordinary manual or scheduled invocations. `--wait`
permits readiness waiting but never bypasses resource limits or resumes a
never-authorized paid restore. The operator can raise execution budgets explicitly.

## 6. Retained state: linear in real obligations, not elapsed history

Use **one compact observation/identity row per distinct source/collection**, with
its latest accepted generation/revision/authority and match flag. A scalar root
generation invalidates old match state in O(1); a new baseline overwrites the same
collection row rather than inserting another generation-by-collection copy.
Immutable identity consistency and admitted paths/suppressions survive. Delayed
old pages/errors must fail the generation/serial fence before touching that row.

Conservatively retain compact lifetime identity/revision evidence: this design
promises O(C), not O(current matches), and does not expire tombstones by age.
A different design may reclaim evidence only with an explicit proof that no
accepted bootstrap, replay, delayed request, pending obligation or future immutable
identity check needs it. CatalogReplica's projection GC is not automatically such
a proof for an accumulating subscription. Do not introduce unsafe TTL cleanup to
advertise a smaller disk bound.

Root operational metadata is O(C + A + M + Q + H) in bounded-row terms, plus actual
variable metadata bytes. Completed placement/receipts, intentional suppressions
and active retrieval/publication work are not presentation history. Compact
terminal attempts to their minimal still-needed outcome/idempotency receipt;
retire that only at the actual replay/remote settlement boundary. An ambiguous
remote attempt cannot be pruned merely because its UI row is old.

Bound presentation history by **bytes as well as rows/age**. Retain the original
30 days / 1,000 finished-run defaults and add recommended defaults of **16 KiB per
serialized summary/progress record, 1 MiB / 64 retained detail records per run and 64 MiB total
retained detail per root and per host scheduler ledger**. Use whichever retention
bound retains fewer finished records. Summary/current attempt entries remain fixed
size; coalesce progress, retain exact counters plus capped diagnostic samples and
explicit `details_truncated` metadata. Never accumulate an arbitrary error or
completed-member array and truncate only after constructing it. These are
presentation caps, not suppression/deletion of semantic work or its exact evidence.
Persist only bounded current progress for an active run; it cannot evade byte
limits by remaining active forever. Native raw stdout/stderr uses bounded rotation
separately; do not rely on the OS default log retention. Downstream backpressure
must not accumulate unlimited progress in RAM.

Cleanup itself is a resumable lane with a recommended **100 subordinate units per
turn**; include every child record, edge, owned file and final parent. Charge fixed
counter bookkeeping separately. Count actual SQL mutations/native work rather than
parents selected. Keep an index on every relevant child foreign key. Do not use
unbounded cascades to implement bounded deletion. Persist ownership before creating
scratch; unknown/unowned or quarantined user files are never garbage. Cleanup
failure is retained and retried without masking the original operation result.

SQLite file footprint is not equal to live row bytes: free pages, indexes,
rollback journals and fragmentation matter. Report logical use, file size and
reclaimable/high-water state distinctly. Reuse freed pages in normal work; no
full VACUUM in idle/status. Any physical shrink operation is separately qualified,
explicit/budgeted and cannot promise O(1) merely from a page-count parameter.
A stable repeated workload must plateau at its necessary high-water footprint,
not grow once per no-op/rebaseline/retry. Quarantine/output bytes are never silently
reclassified as disposable to make that graph look bounded.

## 7. Capacity and storage exhaustion

Publish typed application-owned execution/capacity settings through existing
configuration discovery. Recommended pending intent admission default: **4,096
unsettled collection intents per root**, configurable. This is a deferred-work
threshold, not a limit on completed collections or a silent selection cap. Select
smaller pages when necessary; at zero free slots service existing work and report
capacity pressure rather than drop a matching observation or spin/refetch it.
If a proposed page still exceeds capacity, commit neither cursor nor its partial
obligations. Increasing capacity or draining work resumes from that continuation.
Document that prolonged deferral may outlast server history and require rebaseline;
no finite queue preserves every expired historical transition under overload.

Byte capacity requires separate accounting for necessary final outputs, retained
partial outputs X, disposable scratch, metadata/SQLite overhead and quarantine.
A complete-collection staging directory can be as large as the collection; it is
not bounded scratch just because it is hidden. Prefer per-file create-only
publication with explicit incomplete collection/receipt state when whole-directory
staging would defeat the chosen capacity contract. This narrows the earlier
unconditional preference for whole-collection publication; no weakened hash or
no-clobber behavior is allowed.

Expose optional operator byte quotas and a configurable free-space safety reserve.
An omitted quota means no additional application quota, NOT infinite capacity or
permission to exhaust the filesystem silently. Report effective quotas/reserve.
Reserve known work before external effects where possible, bound unknown metadata
incrementally, and fail/roll back safely on actual ENOSPC. Metadata/journal safety
headroom and cleanup must not depend on admitting more payload work. Fixed RAM
budgets include SQLite cache, decoder, spooled-sort, transfer and progress buffers;
configure disk-backed sort spill and account for its bytes. `temp_store=FILE` is
not a proof by itself. Peak transfer RAM is proportional to c*chunk plus w bounded
descriptors, not B or the complete inventory.

Retry one exact partial-output attempt in place or through bounded replaceable
segments, not one collection-sized copy per retry. Local quiescent resume does
not re-enumerate all completed outputs. Retained partial output is O(X), where X
is a real necessary extent up to selected output size. A file larger than a chosen
operator capacity is deferred with the required amount; it is not truncated or
called invalid. Host schedules inherit these same limits and completion semantics.

## 8. Required proof at integration

Attach declarations to the real bodies in `riverhog-client`,
`riverhog-materialization`, provenance owners, `a-riverhog-cli` and scheduling
components. Use the existing extent annotations/discovery and growth declaration
library; no new acceptance registry or copied promise table on main. Compose
library costs and callback multiplicities explicitly. A bare asymptotic `big_o`
declaration needs reasoning; it is not accepted by the numeric sample validator.
Quantitative upper/equal bounds require independent primitive observations.

Measure the actual maintained bodies: HTTP call/metadata byte counts; iterator
pulls; row/index visits or supported SQL instruction counters; full-scan/sort plan
changes; bytes supplied to hashes, copied and staged; retained rows/bytes/free
pages; live buffers/heap/RSS; child deletions; and sample/log retention. Query plans
alone do not prove work. A SQLite VM count is useful but excludes B-tree page I/O,
RAM and kernel latency. Do not extrapolate its constants to PostgreSQL or another
SQLite build without qualification. Scale independent dimensions and contrast
many tiny members with few huge members and shared/long provenance.

Positive/adversarial witnesses must include: increasing completed roots with fixed
sparse/zero due work; empty advancing pages; changes unrelated to this tag; backfill
across page boundaries; deep seek pagination and budgeted filters; same-timestamp
fan-out; repeated rebaseline of the same identities; suppression/tombstone survival;
large byte streams split across runs; interrupted range/checkpoint/last-hash work;
capacity pressure with cursor rollback; a poison head and cold wait; millions of
subordinate rows behind one parent; repeated logs/retries reaching count/byte caps;
disk-full and abandoned scratch; and multi-root/schedule multiplication.

Include mutation sensitivity: dropping the ready index, adding deep OFFSET,
rehashing the completed prefix, making a generation copy or introducing CASCADE
must trip the corresponding growth/extent witness. Merely adding a decorator to
unexecuted source does not count. Tests run at actual owner gates with exact source
and declaration digest bindings; extent candidates remain nonnormative evidence
associations. Performance timings go through `make profile`/current measurement
rules as observations, not thresholds or permission to weaken safety.

The accompanying prototypes exercise only some of these obligations: real SQLite
queries/deletions/counters, capacity rollback, bounded iterator pulls, generation
compaction and byte turns. They do not implement the real reconciler, real range
custody, source authorization, native schedules, output publication or OS resource
qualification. The integration issue remains open until those owners supply the
required evidence at the actual integrated SHA.
