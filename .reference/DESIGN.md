# Bounded Core stepping: design decision and reference

## Recommendation

**Retain the current one-step runtime. Do not place a Native loop behind its existing accounting or skip completed preparation calls on the strength of a cached ready result.** Several unchanged Native steps can be composed into a finite Core admission, but the current sources do not prove the intended reduction in prerequisite work safe. Deferred catalog adoption adds another set of unresolved exit and recovery rules.

The narrower future candidate is **A: an explicitly admitted group of complete existing Core units, using the unchanged single-step owner and catalog adoption after each owner call**. Preserve every fresh preparation/source check. Its immediate possible saving is intermediate outer stamps; prerequisite and owner work still repeat. A larger scheduling quantum, cancellation responsiveness and finalization-scheduling boundary require explicit owner decisions even for A. The proposed default remains one attempt. **B: multiple Native calls inside one owner call with final-only catalog adoption remains outside the initial recommendation.**

This packet is a source-owned design reference, with no runtime patch or candidate execution. The current declarations, source and normal integration rail remain authoritative. [Lifetime reasoning](LIFETIMES.md), [growth/resource model](GROWTH.md), and [focused receiving matrix](VALIDATION.md) supply the detailed proofs and unresolved cases.

## 1. Reconcile the receiving checkpoint first

The exact audited base is **d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e**. Relative to 3b47cbba770c31eb1487b50f7e3b16533c10e827, only two tracked files changed: Native `_session_checkpoint.py` and its owning checkpoint tests, with 50 insertions and 10 deletions. The private prefix read now returns its exact fresh row and the parsed prefix used to validate it. Initialization/read/fence consume that parsed value within the same invocation; publication independently parses its selected prefix. Fresh SQL, canonical checks, predecessor/worker/source/algorithm predicates and transaction/CAS boundaries remain. The nine new warm-corruption cases cover noncanonical, source-position and unknown-field corruption across load, fence and publication. [Exact audited delta](evidence/audited-prefix-change.patch).

Core, the API/SDK callers, Native phase logic, growth reader/helper, relevant qualification definitions, guides and lock files are unchanged. The [source audit](evidence/source-audit.json) pins the source and inspected files. This supports carrying forward the earlier structural phase analysis; it does not make old instrumented timings measurements of d4d7.

The [later receiving report](https://github.com/nashspence/riverhog/issues/991#issuecomment-6889181049) supersedes the request's pending-CI status. It reports d4d7 fully qualified on tested merge 05abffaef7f87a93b4949fcd6981e4ce6d033864: 31 jobs, 26 workloads, 7,579 selected supplemental cases, 48 scoped witnesses, 54 executions and 1,219 top-level samples. It also reports exact-input parity across the 479 Native traces while prefix decodes fell from 5,185 to 3,758 and all 1,903 parent-checkpoint parses remained. These are receiving-owner reports, not newly executed or independently requalified evidence in this packet. Its differently conditioned lane is not compared with the earlier producer timing.

## 2. The present boundary is larger than one Native call

The [stamped Core entry](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L2122-L2177) takes one before stamp, runs one unit, then takes an after stamp on normal return. The returned boolean is their actual inequality. A child exception propagates without an invented after stamp.

That unit can advance capture, reconstruct and verify the selected captured source, then advance/check epoch, topology, union, rank and replay preparation. Only after readiness does it invoke `UploadJournalValidationOwner.advance` once. The owner reserves its original slot, opens/adopts if necessary, reads current progress, executes one Native step, adopts progress under catalog fences, and receives terminal resources. Core terminal journal publication remains a separate fenced operation. [Current owner control flow](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_validation_owner.py#L2029-L2122).

The owning growth declarations independently cap the service's owner call at one and the owner's Native call at one. The owner invariant explicitly says only one Native unit executes. The outer stamped declaration has `unit_calls_limit = 1` and an exact single-child input map. A loop under those unchanged declarations would hide work even if each Native call remains individually bounded. [Current and proposed multiplicities](GROWTH.md#1-existing-ownership-and-call-multiplicities).

### Freshness map

| Boundary | Checks that must remain with its owner | Frequency for conservative A |
| --- | --- | --- |
| Captured source selection | Exact attempt/directory/version/bytes/hash/cursor/failure, directory integrity, last captured page digest | Each attempted Core unit before Native eligibility |
| Completed preparation | Last epoch Frame, topology Fact, union Fact/Arc, rank vertex/arc/graph-result links; algorithm/cursor/extent checks | Each prior readiness boundary, unless an equivalent fresh validator is separately proved |
| Retained slot/open | Exact source/catalog equality, nonblocking slot reservation, original workspace lease before worker claim, restore/adopt without recreating lost accepted state | Existing owner path on each call; opening only when actually required |
| Native entry and source reads | Fresh worker/source/replay-binding/certificate checks, exact binding and range rules | Every unchanged Native step and actual source read |
| Native checkpoint | Fresh ledger load, worker/prefix/predecessor checks, transactional fence and conditional parent publication | Every unchanged Native phase transaction |
| Catalog progress adoption | Current source fence, original job identity/epoch, latest slot job version and previous progress CAS | After each existing owner step, and the existing open/failure paths |
| Terminal journal publication | Exact result/source/binding/anchor and current terminal source fence | At first terminal outcome, before returning normally |

The callback and preparation predicates are not interchangeable. They also have separate observation costs. Full source links and the exact map are in [LIFETIMES.md](LIFETIMES.md#1-what-the-current-unit-actually-owns).

### Decisive counterexample: unchanged manifest, missing dependency

After a successful step, delete the completed rank preparation's last graph-result row while leaving its Scan manifest and source/worker metadata unchanged. The next existing Core readiness call executes `rank._begin`, reads that result and raises if it is absent. The certificate's ordinary `verify_current` joins Scan and Journal manifest fields, not that dependent result row. A following non-graph Native phase can therefore pass the narrower callbacks and perform work after a boundary the old Core route would reject.

This is a source-derived interleaving, not a new executed failure. The maintained [rank corruption test](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/unit/test_journal_epoch_rank.py#L306-L351) explicitly deletes the completed graph marker and requires rejection. [Rank predicate](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_rank.py#L218-L291); [certificate predicate](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_certificates.py#L735-L751).

Equivalent distinctions exist for other retained preparation links and captured-page integrity. Completed epoch readiness can also perform bounded retired-preparation cleanup. Thus “already complete” does not mean either zero work or permission to reuse an earlier acceptance. Omitting these calls needs a preparation-owner design for an equivalent fresh validator or a justified different lifetime, with corruption and error-order evidence. Header equality alone is insufficient.

## 3. Compare the available designs

| Design | Potential reduction | Preserved boundaries | Decision |
| --- | --- | --- | --- |
| Current one-step route | None from grouping | All existing admission, readiness, adoption and scheduler boundaries | **Retain now** |
| A: complete Core units inside a finite admitted group | Intermediate outer stamp invocations; no prerequisite/owner saving is established | Fresh readiness and existing owner adoption/Native commits per step | Conditional design candidate; needs new parent accounting and coordination policy |
| A with a newly factored fresh readiness validator | Possible duplicate preparation/control work inside each boundary | Must prove every old predicate, failure ordering and cleanup obligation | Separate preparation-owner work; no validator is implemented here |
| B: several Native steps followed by one adoption | Potential outer and owner coordination reduction | Native phases/commits could remain, but adoption/conflict/exit boundaries change | **Leave out of the initial change** |

Even A is not a drop-in loop: the current service unit returns `None`; it does not expose whether it is safe to continue. It can perform preparation-only progress, return without Native work, recover a prefix during opening, or complete a terminal outcome. A new internal control disposition is required. It would describe continuation, not certify source acceptance or replace actual stamp observation.

## 4. Explicit admission and work accounting for A

### Attempt and Native reservations

Let **S** be a positive, explicitly admitted maximum of attempted complete Core units. Validate it before effects and consume a credit before each attempt's capture/preparation work. Count preparation-only actions, busy/stale results, capacity refusals and failed attempts. A counter incremented only after successful Native progress is not a work bound. Do not refund a refused attempt into an internal retry loop.

The proposed default is **S=1**, retaining current behavior. Selecting a larger scheduling quantum is an explicit Core/runtime/extent decision. This packet uses **S=2 only to illustrate the accounting**; it adds no hidden setting or archive/member cap.

Each possible Native call keeps its own existing `JournalValidationBudget(R_i, B_i, W_i)`: row/semantic-step credits, work octets and source-window octets. Preserve the existing exact-integer validation, `1 <= R_i <= 64`, positive work bytes, and positive source window fitting the work budget. Admit a finite prefix whose reservation sums fit the caller's aggregate ceilings. Do not pass an aggregated 128-row budget to a child whose limit remains 64. [Existing Native budget](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/journal_session.py#L66-L79).

| Native reservation | One ordinary possible call | Illustrative two-attempt reservation |
| --- | ---: | ---: |
| Row / semantic-step credits | 64 | 128 |
| Work octets | 33,619,968 | 67,239,936 |
| Source-window octets | 65,536 | 131,072 |

These are reservation totals. They are not measured consumption, complete Core byte bounds, heap/RSS limits or a bound on all source delivery. Pending exact-frame admission has its own atom/work-byte condition; its bytes are not all covered by the window column. Capture/preparation, fresh guards, full metadata/error widths, indexed populations, callbacks, opening and receiving retain separate source-owned envelopes.

### Every actual child remains visible

For `a <= S` attempted units and `n <= a` actual owner calls, the conservative direct-edge caps are:

| Child group | Aggregate direct cap |
| --- | ---: |
| Complete service units / owner calls / Native advances | Each at most S |
| Framing / topology planning / topology advance | Each at most S |
| Epoch classification records | 64S |
| Owner sweep / reserve / open / Native / result | Each at most n |
| Owner progress / publish / drop | Each at most 2n |
| Owner currentness callbacks | 4n |
| Owner window / exact reads | Each at most n; their combined count at most n |
| Certificate edge callbacks | Sum of row budgets over actual owner calls |
| Normal group before/after stamps | 2; an exceptional child prevents the after stamp |

These are edges in existing declarations, not flattened counts of all descendants. `_open` has separate nested progress/publication/lease/source/construction work; `_drop` and source/certificate operations retain their own children. An actual cold open or retained reader cannot disappear because a group is usually warm. Recursive preparation functions and the service's exclusive Core primitives remain accounted in their current owning domains. The [complete inventory](evidence/growth-inventory.json) and [GROWTH.md](GROWTH.md) identify every direct current cap and its owner.

For each primitive m, compose **exclusive parent work plus the inclusive work of actual non-overlapping children**. Keep an independently enforced structural call cap as well as each actual child count. Each child keeps its own arguments, input envelope and independent validation; changing retained populations require valid later inputs, not a copied first-step sample. Rejecting children and children completed before a later failure remain in the record. Native/provider callbacks follow the existing metric ownership so raw inclusive Native, Core and callback totals are not added twice.

The new parent also pays for its own admission, control disposition, exact-source identity comparison, loop decisions and returned metadata. Its exclusive work cannot inherit the old straight-line `unscoped_operations = 0` assertion without review. Full comparison/error widths matter; a fixed number of calls does not make them constant-sized. Error-message truncation is not a bound on formatting the original error.

### Resource admission is distinct from cumulative work

Use the existing original `JournalConstruction`, storage admissions and workspace lease. A acquires/releases the existing per-key execution lock on each owner call; it does not create an additional executor, increase retained-slot capacity or hold the pool mutex during work. Catalog readers/transactions must close before Native work; no transaction may span the group of Native commits.

Current Core defaults are two retained slots and separate `JournalStorageGrants` of eight connections, 32 cursors, four operations and two file records per construction. Operation/cursor tickets return at their own receiving boundaries. Retained storage/connection/lease custody can continue through independent readers. Sequential grouping must neither multiply simultaneous limits by S nor treat a fixed peak as a bound on total opens, closes or work. A pending or unknown close retains the exact original and its charge.

Do not accumulate S result readers, full checkpoint copies or input buffers for convenience. Retain only bounded control necessary for the current group; detailed diagnostic samples belong to the explicitly accounted observer. Full metadata widths remain charged. These limits preserve resource ownership, not a whole-process memory proof. [Resource and lifetime analysis](GROWTH.md#3-proposed-admission-contract-for-a).

## 5. Control-flow and adoption boundary

The following is deliberately non-executable. The named disposition helper does not exist in the source and is not an authorized patch:

```text
admit positive finite S and complete per-attempt work/resource envelopes
before = actual catalog stamp
for each admitted attempt, at most S:
    consume one attempt credit before capture/preparation effects
    run one complete existing Core unit with an explicit disposition:
        repeat all current capture/source/preparation checks in owning order
        invoke the unchanged owner once if ready
        retain its per-step Native transaction and catalog adoption
        perform existing terminal journal publication if an outcome exists
    stop unless the result is repeatable nonterminal Native progress
    stop at source/attempt replacement or loss of further admission
    honor the source-owned scheduling/cancellation/finalization boundary
after = actual catalog stamp                 # normal return only
return before != after
```

The helper must preserve all existing early returns and exception order. For later attempts, the fresh selection must still belong to the admitted exact source/attempt before new preparation or Native work is authorized; replacement ends the group and needs new admission. Source identity comparison is not cached currentness. Existing owner calls continue to reserve and verify actual worker/job state normally; the group does not confer authority on a saved worker or old job version.

Stop on preparation-only work, `False`, a terminal success/failure outcome, an exception, exhausted allowance, or the caller's defined loss of admission. A preparation-only turn may have committed progress and still must stop. A cold open can adopt recovered Native state then return `False` on capacity. Keep the real final stamp comparison; neither the last child flag nor an OR of flags replaces it.

Preserve exception propagation without a fabricated after stamp or success result. Do not adopt in an unconditional `finally`: the source/worker may be stale, the failing operation's durable outcome may be unknown, or original receiving may already be underway. Do not continue after terminality or retry capacity internally with unchanged admission.

### Why the initial design keeps per-step catalog adoption

The current job publisher permits monotonic Native version/frame jumps, but separately checks the exact source and job identity, worker epoch, current catalog job version and previous progress. Worker currentness intentionally omits the saved job's old version/progress. Representing a jump is therefore possible; choosing to defer adoption changes when conflicts and partial work are observed. [Job publication contract](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_validation_jobs.py#L463-L544).

A warm B loop can commit Native version v+1, then encounter capacity on its second call. Reusing the current capacity handler returns `False` with no adoption of v+1; the outer catalog stamp may stay unchanged. Repeating the same inadequate admission can keep pausing without reporting the earlier commit. A preserves the first step's existing adoption before entering the second call. This distinction is about reporting and recovery, not a claim that Native bytes were lost.

Likewise, a competing job-version change may pass worker-current identity checks but fail the publication CAS. Per-step adoption detects it after the next step; final-only adoption can consume several more steps first. An owner decision and complete failure-boundary proof would be required before B could change that detection window.

## 6. Required exits and original receiving

| Event | Required initial-design behavior |
| --- | --- |
| Warm capacity on a step | Stop without a busy retry; preserve the same unfinished atom under its existing path. Earlier A steps are already adopted. Preserve capacity versus SQLite/provider error distinctions. |
| Cold open then capacity | Initial/recovered progress may already be adopted. Stop, retain/receive the original as its owner requires, and let the actual stamp reflect the change. |
| Source, worker or certificate replacement | Fresh refusal stops the group. Old work cannot publish to the replacement; preserve original closing/lease/resource custody. |
| Semantic rejection after earlier progress | Preserve the first actual failure, existing fresh progress selection and failed-outcome publication; stop before another Native step. |
| CAS loss or catalog exception | Do not refetch a new job and overwrite it. Preserve the actual rejection/unknown outcome and existing receiving; Native can remain ahead for normal recovery. |
| Confirmed process exit between Native commit and adoption | After the normal ownership path acquires the exact workspace lease, reopen the same workspace under a new worker epoch and adopt the verified committed prefix before more work. Never recreate an active missing database. Existing recovery is architectural support, not proof of all grouped exits. |
| Reply loss or an unknown execution outcome | Missing acknowledgment does not prove the original call stopped or failed to adopt. It may still hold the slot lock/lease, or already have published durable progress. Retry through the existing owner path and inspect current durable state; replacement must wait for original execution/receiving and successful exact-lease acquisition. Do not force a new worker epoch from reply loss. |
| Terminal outcome followed by uncertain cleanup | Keep the confirmed job outcome and the exact closing slot/construction/storage/lease charged. Retain independent readers until original receiving confirms. Cleanup failure can prevent a normal return. |
| Terminal reply loss before final journal publication | Respect original execution/receiving, recover any durable job outcome through the normal owner path, and perform independently fenced terminal publication. Do not replay accepted semantics. |
| Cancellation while an operation is entered | A request is not receipt of termination. Keep original execution/custody until the operation and receiving finish. The current owner has no cancellation token, so a finite loop alone cannot promise per-step cancellation responsiveness. |
| Busy key, full pool or another ready journal | No blocking reservation or duplicated worker. Stop/yield under the chosen quantum; unchanged capacity is not an executor or proof of fairness. |

The detailed [exit matrix](LIFETIMES.md#4-exit-interference-and-recovery-matrix) retains source deletion, unknown close outcomes, durable restart and independent-reader distinctions. These are source-derived obligations; no new fault injection ran here.

## 7. Fairness and finalization are explicit design choices

The existing scheduler rotates candidate selection, permits at most one progressed candidate per outer iteration, and alternates retirement opportunities. With caller iteration allowance L, retained-slot capacity C and proposed attempt allowance S, its existing candidate limits yield the loose structural bound **Native attempts <= L(2C+1)S**. Every actual attempt and child remains recorded. This is a logical bound, not a wall-time guarantee or evidence of parallel utilization.

Increasing S increases the amount of selected-key work before another candidate opportunity. Preserve cursor rotation, retirement opportunities, nonblocking reservations and return at exhaustion/refusal. Do not claim the same fairness quantum merely because concurrency remains two. Selecting S and any caller-specific grant, plus the treatment of cancellation between completed steps, belong to the Core scheduler/control owner. The present owner API has no cooperative cancellation parameter; this packet invents no successful cancellation receipt.

There is another caller boundary: `process_due_provenance_journal_validations` calls `_schedule_finalization_if_ready` after each progressed candidate. Combining old candidates under one stamp pair can defer those opportunities. The readiness predicate examines planner closure, payload seal and sealed-object count; it does not directly require the selected journal to be terminal. An intermediate check cannot therefore be dismissed as a guaranteed no-op. Preserve that opportunity or explicitly justify its new bounded placement and account for its work. [Scheduler](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L2209-L2251); [scheduling effect](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L3618-L3667); [readiness predicate](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L7369-L7386).

For a normal group of a attempted units, two outer stamps instead of two per unit could remove **2(a−1) stamp invocations**. That is the defensible call-count opportunity under fully preserved prerequisites, not a measured SQL/time saving. Additional continuation checks or preservation of intermediate scheduling can reduce that opportunity. No throughput benefit is established here.

## 8. Growth first, without calling the remaining work constant

Grouping changes a coordination coefficient. It leaves the sum of actual Native phases/transactions, preparation, source bytes, metadata, graph work, retries, cold reopening and receiving. Completed preparation still performs fresh indexed checks and sometimes bounded cleanup; preparation algorithms themselves must retain their input-dependent total-work arguments.

Correction work remains distinct: retirement/reseed/revalidation can revisit the effective projection, so cumulative work includes the projection sizes and graph/semantic work at each correction. The earlier tiny correction rechecked six active assertions after one replacement and had no nonzero adjacency traversal. It does not qualify DAG growth, a general correction bound or G10/G12/G13. Step grouping does not fix repeated whole-resource work or permit charging it as a constant.

Do not claim a saving from rebuilding the admission catalog on every call: its construction is already cached, and its checkpoint digest is already retained. Fresh authority checks, query construction, codec work and catalog identity construction are different costs. Primitive/VM bounds remain distinct from physical I/O, lock/fsync latency, allocator costs and complete process memory. [Growth analysis](GROWTH.md#5-fairness-scheduling-and-cumulative-work).

The later receiving report localizes all 418 historical catalog raises to finalization but does not supply exception types/codes, statement families or retry identity. Their cause and cost remain unclassified. Native transaction cost, transport/lifespan overhead and actual ready-key/worker occupancy remain separate open questions; this design supplies no new measurement of them.

## 9. What the owning issue must decide

1. **Retain S=1 now, as recommended, or develop the limited A candidate.** Decide whether saving intermediate stamps warrants a changed coordination quantum. No runtime patch in this branch makes that choice implicitly.
2. **Preserve all readiness checks or commission an equivalent fresh validator.** The preparation owners must enumerate the old predicates, error order, cleanup and invalidation behavior. A previous ready result or unchanged manifest is not sufficient proof.
3. **Choose the quantum and caller boundaries.** Define the attempt/work/resource grant, exact-source continuation, fairness, cancellation and intermediate finalization scheduling. Keep phase fusion, commit removal and changed concurrency outside this initial scope.
4. **Keep catalog adoption per step for the initial design.** Any move to B needs the larger Native-ahead exit, conflict and receiving proof before implementation.
5. **Change declaration/observation composition with the code.** Preserve all existing single-step witnesses, add actual multi-child/late-failure receiving where the chosen design needs it, and propagate the new parent through service/caller/scheduler accounting. Do not weaken one-step assertions to make a loop fit.

The [20-case receiving matrix](VALIDATION.md#3-focused-counterexample-and-receiving-matrix) identifies discriminating boundaries without prescribing a broad campaign. A later concrete candidate could use two or three actual Core turns and, for fairness, two independent journals under one approximately 120-second cooperative workload cap. It would retain all actual child/primitive records, exact partial or terminal results, original exits and original receiving. An incomplete prefix would be reported as partial; a deadline would stop new admission without declaring an entered owner finished.

**No such diagnostic is needed now to establish the present counterexamples, and none ran.** A small valid success could not authorize omitted guards, deferred adoption or a new fairness policy. The branch contains static design/evidence only, with product paths unchanged at d4d7. Whole-path #964 acceptance, G10/G12/G13, large-campaign holds and the absence of a timing gate remain as requested.
