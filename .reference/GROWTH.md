# Bounded Core stepping: growth and resource ownership

**Status:** static design/reference, not an implementation or a new product contract. This note inspects exact source `d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e`. No product operation, fixture, test, profile, or diagnostic was run for this audit. Source-owned declarations and the existing ordinary runtime remain authoritative.

## Recommendation

Leave the current one-step behavior in place. If the owner chooses to develop a bounded grouping, the narrower candidate is **A: repeat the existing single-step owner operation, preserve its catalog adoption after each step, and preserve every existing preparation/currentness check**. Its first possible saving is intermediate outer stamp calls. It does not yet justify skipping prerequisites. **B: several Native advances inside one owner operation followed by coalesced adoption** requires substantially more failure, freshness, and receiving proof; it should remain out of this change.

The present service-unit declaration permits one `UploadJournalValidationOwner.advance`; that owner declaration permits one `JournalValidationSession.advance` and explicitly states that only one Native unit executes. A loop cannot run under either unchanged declaration. A new parent must independently cap all attempted turns and compose every actual child, including unsuccessful calls. [S1–S3]

The prerequisite functions are not just redundant readiness flags. They freshly check retained dependent rows, including the last frame, topology fact, union fact/arc, and rank vertex/arc/result. The source/certificate currentness checks inside Native do not independently re-read all these rows. Removing the prerequisite calls therefore needs a separate dependency-lifetime or immutable-authority proof, or equivalent fresh checks at the same rejection boundary. A manifest that did not change does not prove that its dependent rows still exist. [S4–S5]

The current Core unit returns `None`, while the outer before/after stamp supplies durable progress. Grouping several units between two stamps requires an explicit internal **continue/stop disposition**. That disposition must not replace the final actual stamp comparison, and must not treat `ValidationAdvance(False)` as evidence that no effect occurred: a cold open can adopt recovered Native progress before a later capacity return. [S1–S2]

The retained Native observations motivate investigating coordination, but do not measure either proposed grouping. The current receiving context reports d4d7 fully qualified, with 48 scoped growth witnesses, 54 executions, and 1,219 top-level samples. Those are receiving-owner reports, not new runs in this audit, and do not close whole-path G10/G12/G13 acceptance. [E1]

## 1. Existing ownership and call multiplicities

### The surrounding service and scheduler

The current normal path takes a before stamp, attempts one service unit, then takes an after stamp and compares four-field durable state. An exception propagates without the after stamp. One unit may perform bounded capture/framing, then epoch/topology/union/rank/replay preparation, then at most one Native owner call and fenced Core terminal publication. Preparation can advance durable state and return before any Native call. [S1]

| Owner/delegate | Existing bound for one invocation | What the number does and does not mean |
|---|---:|---|
| Stamped service advance: `stamp` | 2 | Before and normal-return after snapshots; not two on every rejection |
| Stamped service advance: `unit` | 1 | Independently attempted unit, including preparation-only/unchanged work |
| Service unit: `framing` | 1 | Work retains the actual byte/frame admission |
| Service unit: `epoch` classifier | 64 | One child record per actual classified frame, not one aggregate classifier record |
| Service unit: `plan` | 1 | Actual topology-frame planning child |
| Service unit: `topology` | 1 | Actual bounded topology advancement child |
| Service unit: `owner` | 1 | Complete retained Native ownership, callback, publication, and receiving scope |
| Scheduler: outer iterations | `L` | Effective nonnegative caller admission, not observed progress |
| Scheduler: fresh selectors | `L(C+1)` | `C` is configured retained-slot capacity |
| Scheduler: stamped validation candidates | `L(2C+1)` | At most `C` warm plus `C+1` fresh candidates per outer iteration |
| Scheduler: stale sweep / active-key snapshot / retirement / finalization schedule | Each at most `L` | Cleanup-first and fallback retirement are mutually exclusive per iteration |

These are source caps, not claims that every child executes at its cap. The service unit's exclusive Core bounds separately include catalog statements and controls, returned rows/metadata, parameters, SQLite instructions, codecs and hashing. They depend on actual catalog population, complete metadata widths, transport extents and authenticated maximum frame pages. Core preparation includes existing 64 KiB/64-frame windows, up to 64 topology items, four union transitions and sixteen rank transitions sharing the existing row budget. A fixed number of function calls does not make their work independent of these dimensions. [S1, S3, S6]

### Direct children of one existing Native owner advance

| Tag | Owning operation | Existing direct cap |
|---|---|---:|
| `sweep` | `UploadJournalValidationOwner.release_stale` | 1 |
| `reserve` | `_reserve` | 1 |
| `open` | `_open` | 1 |
| `progress` | `JournalValidationSession.progress` | 2 |
| `native` | `JournalValidationSession.advance` | 1 |
| `result` | `JournalValidationSession.result` | 1 |
| `publish` | `jobs.publish_progress` | 2 |
| `drop` | `_drop` | 2 |
| `current` | `_OwnedSource.verify_current` | 4 |
| `window` | `_OwnedSource.read_window` | 1 |
| `exact` | `_OwnedSource.read_exact` | 1 |
| `certificate` | `UploadJournalGraphCertificates.edge` | `row_budget` |
| combined `window + exact` | Source-read admission | 1 |

These are **direct declaration edges**. For example, cold `_open` has its own construction/source/certificate/job/lease/Native-open/progress/adoption children; its nested progress and publication are not the direct `progress` and `publish` rows above. `_drop`, result borrowing, source reads and certificate operations likewise retain their own nested obligations. Adding direct-edge counts to all descendants as though they were disjoint work would be incorrect. [S2]

The ordinary owner performs a fresh `current.progress()`, one unchanged `current.advance(budget)`, then catalog publication. A caught Native provenance/value rejection reads the accepted progress again and publishes a detached failure; capacity retains the unfinished warm atom; stale and failed custody take their existing receiving paths. The owner’s bounds include full error-control formatting width: the later 1,000-character message truncation is not a formatting-work bound. [S2]

### Native child structure remains unchanged

Each `Session.advance` checks source currentness, loads the fresh ledger and admits the current budget before dispatch. Its direct delegates include ledger load, progress materialization, either entry advance or entry admission, ledger fence/publication, framing and hash padding. Depending on phase, `EntryReplay.advance` performs another fresh load/start and selects at most one row, lifecycle, semantic child, graph, recording or frame-publication branch. Source input may instead scan, synchronize, admit a pending exact frame, or complete. None of those phases or their durable transactions may be fused by a Core grouping. [S7–S8]

Thus `n` Native calls do not mean `n` SQL calls, `n` rows, or `n` JSON operations. Child row/semantic budgets, actual input/control widths, graph adjacency, indexed populations, source deliveries and branch-specific nested calls remain separately charged. The static entry-conductor declaration explicitly preserves these child domains and inclusive primitives. [S7–S8]

## 2. Compare the two designs

| Property | A: repeat unchanged owner calls, per-step adoption | B: internal Native loop, coalesced adoption |
|---|---|---|
| Native phases and commits | Unchanged per actual owner call | Must remain unchanged per actual inner Native call |
| Owner currentness/progress/publication | Repeated unchanged | Some existing owner observations would move or disappear; requires independent equivalence proof |
| Catalog adoption | Each successful current owner call publishes its exact progress | Deferred publication must correctly handle every early exit and accepted prefix |
| Native-ahead window | Existing interval between one Native call and its owner publication | Potentially several committed Native calls; current one-step recovery tests do not establish all such exits |
| Slot admission | Existing nonblocking reserve and release on every owner call | One longer slot ownership interval; no global lock may cover Native or catalog work |
| Preparation checks | Preserve every call unless an owning lifetime proof authorizes a narrower fresh equivalent | Same prerequisite gap; Native currentness alone does not resolve it |
| Immediate expected saving under conservative freshness | Intermediate outer stamps; no claimed saving in prerequisites or per-step owner controls | Potential outer and owner coordination saving, not yet established as safe or measured |
| Necessary declaration change | New bounded parent; existing owner declaration still says `native <= 1` | New/changed owner declaration and multiplicities as well as parent composition |
| Recommendation | Only candidate worth further source-owned design; retain default one attempt | Leave out pending partial-progress, error-order and lifetime proof |

For B, a concrete counterexample to a naive loop is: one or more inner Native calls commit; a later inner call raises capacity; the existing handler returns `False` without publication. The warm Native state has advanced, while the outer catalog stamp can remain unchanged. Repeating the same insufficient budget does not adopt that prefix, and the scheduler can conclude that nothing progressed. Existing single-step capacity tests deliberately assert one Native call, no publication/drop, an unchanged Native snapshot and unchanged catalog job. Those assertions cannot simply be extended to a multi-call prefix. [S2, S9]

`publish_progress` accepts monotone Native version/frame jumps and uses the previous exact job version/progress in its compare-and-set. That permits discussing coalesced adoption; it does not establish correctness for capacity, stale replacement, failed recording, cancellation, reply loss, late result validation or original receiving. `worker_current` checks current owner identity/state, not whether every intermediate Native progress was adopted. [S10]

## 3. Proposed admission contract for A

This is a proposed contract shape for the owning source, not permission to add a loop now.

1. **Independent attempt count.** A positive integer `S` limits attempted Core turns before any progress is known. Consume one credit before the turn’s capture/preparation/owner work. A preparation-only return, busy reservation, stale return, capacity rejection, or failing child consumes the attempt. A counter incremented only after successful Native progress would not bound those other paths.
2. **Explicit Native reservation.** Each potentially admitted Native call receives its ordinary `JournalValidationBudget(R_i, B_i, W_i)`. Existing validation remains: `1 <= R_i <= 64`, `B_i > 0`, and `1 <= W_i <= B_i`. For an aggregate reservation contract, admit only a finite prefix satisfying `sum R_i <= R_unit`, `sum B_i <= B_unit`, `sum W_i <= W_unit`, as well as the attempt limit. Reserve before calling, and do not refund a rejected attempt into an unbounded retry loop. A fixed `S` times the unchanged ordinary budget is a simple conservative reservation form. [S7–S8]
3. **Core work stays separate.** Capture, epoch/topology/union/rank/replay preparation, source/worker/certificate checks, catalog transactions, full controls, opening and receiving keep their own envelopes. The Native budget does not pay for all these operations. Parent dimensions must dominate the actual heterogeneous child inputs, including rejected controls and changing retained populations.
4. **Finite stop rules.** Stop after the first preparation-only/non-repeatable turn, `False`, terminal outcome, loss of selected source/attempt identity, or exception. Stop after the last admitted attempt even if progress continues. Do not immediately retry capacity with the same credit. Preserve terminal Core publication before returning. All exceptions and original receiving retain their current ownership.
5. **No concurrency or resource substitution.** The step allowance is distinct from retained-slot capacity, storage admission and reader custody. It grants neither more active slots nor new connections/threads. It cannot release an original because a later turn is available.
6. **Default and authority.** Preserve the current/default one-attempt behavior. A deployment-dependent allowance that affects scheduling or accepted work needs an explicit Core/operator decision, validated configuration and source-linked extent semantics. Neither this note nor a hidden local constant may establish an archive/member limit. [S11–S12]

The ordinary Native reservation is 64 row/semantic-step credits, 33,619,968 work octets and 65,536 source-window octets. For the **illustrative** `S=2`, conservative sums are 128, 67,239,936 and 131,072 respectively. These are reservation totals, not actual consumed work, complete Core byte bounds, allocation, retained memory or peak RSS. In particular, the source-window allowance is not a cap on all source bytes: a pending exact frame follows its separate actual-frame/work-byte admission. Whole-row/metadata/control checks and database work also retain their declared envelopes. [S7–S8]

Current resource grants illustrate the separate ownership: Core defaults to two retained validation slots, while `JournalStorageGrants` independently supplies eight connection, 32 cursor and four operation tickets, plus two file records per journal construction. Each grant remains charged until its owning operation or resource is received: a completed operation releases its operation ticket, a cursor follows its actual closure, and retained storage/connection/lease custody can continue through independent borrowed-reader lifetimes. Their values are not a sequential-turnover allowance or an aggregate heap/scratch limit. Native retains FILE temporary storage and its existing durability profile. [S11]

### Control-flow sketch, deliberately not executable

```text
before = actual_stamp(selected_journal)
for each independently admitted attempt up to S:
    reserve this attempt and its possible Native work ceilings
    disposition = one_existing_unit_with_explicit_disposition(
        selected_journal,
        repeat_only_the_same_selected_attempt,
        unchanged_per_step_native_budget,
    )
    # Every old capture, prerequisite, owner, adoption and terminal-publication
    # action still occurs in its original order; every child is retained.
    if disposition is not repeatable_nonterminal_native_progress:
        break
after = actual_stamp(selected_journal)
return before != after
```

`one_existing_unit_with_explicit_disposition` does not exist today. It would preserve the old code’s early returns and exceptions while exposing whether the owner completed a repeatable nonterminal turn. It must stop after preparation-only work, even if that work changed the stamp, and after an owner `False`, even if cold opening changed the stamp. The final stamp still reports real durable change. An exception still propagates without inventing an after stamp or successful return. [S1, S3]

Removing intermediate finalization/readiness scheduling is also an owning behavior change: the existing scheduler schedules after each progressed candidate. `_ready_for_finalization` checks a closed planner, complete payload seal and sealed-object count; it does not directly require the selected Native journal to be terminal. Intermediate scheduling therefore cannot be assumed to be a no-op during Native progress. A grouped definition must preserve that opportunity or explicitly justify the changed boundary and include the work it defers. This is an additional reason not to adopt a runtime loop from this static note. [S3, S15]

## 4. Total and inclusive composition

For a primitive `m`, a proposed parent can use the form

`Parent_m = ExclusiveParent_m + sum over actual, non-overlapping child invocations ChildInclusive_m(inputs)`.

For a compact declaration this may be bounded by each actual child-call count times a source-owned bound envelope over dominating inputs. That is the pattern already used by the service and owner declarations. The independent structural caps must also hold; actual-call dimensions alone cannot authorize arbitrary work. Do not use observed SQL, allocation or elapsed time to invent an input envelope. Absent children can use zero; present rejected children cannot disappear. [S2–S3]

For conservative A with `a <= S` attempted old units and `n <= a` actual owner calls:

- `unit_calls <= S`, `owner_calls <= S`, `native_calls <= S`.
- Framing/planning/topology child calls are each at most `S`; epoch classifier calls are at most `64S`.
- Composing the old direct owner caps safely gives sweep/reserve/open/native/result each at most `n`; progress/publish/drop each at most `2n`; currentness callbacks at most `4n`; window/exact each at most `n` with combined source reads at most `n`; certificate calls at most `sum R_i` over the actual Native owner invocations.
- These are deliberately conservative aggregate caps. They are not a claim that an uninterrupted warm batch opens or finishes `n` sessions. Any tighter one-open/one-result claim needs its own stop/lifetime proof and must keep nested `_open` and `_drop` children distinct.
- Keep two outer stamps on normal completion and one unit comparison. The existing single-unit exact input map must become actual per-unit child maps plus valid envelopes; increasing `unit_calls` alone is insufficient.

**Metric ownership is essential.** Service `core_*` primitives are exclusive of its Native owner child. Native session/workspace SQL covers its own SQLite owner; provider/caller bodies execute normally but remain with Core source/certificate owners. The maintained recorder suspends the appropriate outer observer around Native work, retains Native child records, and reconciles remaining native-storage work by subtracting recorded child totals. It separately preserves callback and lifecycle effects. Summing a raw inclusive Native interval, an inclusive owner interval and all nested callbacks would count the same operations more than once. Grouping must extend the current declaration/recorder structure, not introduce overlapping totals. [S7, S13]

Primitive attempts also differ from semantic success. Retain complete samples for all completed, rejected and capacity attempts before a later failure. A final parent sample must not report only the accepted prefix or final child. All dimensions, per-edge counts, Native SQL/codec/source operations, result-reader activity, commits/rollbacks, catalog effects and original receiving remain attached to the actual invocations. Recording/validating the observer’s own metadata stays outside primitive observation windows. [S9, S13]

No proposed row/byte credit replaces indexed-population terms or full metadata extents. A long valid/invalid control field can increase codec/hash/comparison work while the number of calls remains fixed. SQLite logical/VM counters do not establish physical I/O, B-tree comparison-byte, lock-wait, fsync, allocator or memory bounds. [S2–S3, S8]

## 5. Fairness, scheduling and cumulative work

The present scheduler rotates `_journal_validation_after`, uses bounded warm/fresh candidate lists, stops at first durable progress, and interleaves cleanup turns. Its slot limit `C` is not a worker executor. For the proposed conservative parent, the existing candidate cap would imply the loose derived bound `Native attempts <= L(2C+1)S`, provided every attempted grouped candidate and every inner attempt is recorded and bounded. That expression is a structural bound, not evidence of concurrency, throughput or a wall-time service guarantee. [S3]

Increasing `S` means another ready journal can wait through more actual work before the cursor rotates. Preserve return to the scheduler at credit exhaustion and every non-repeatable boundary, preserve the cleanup turn, and keep busy/unchanged candidates from preventing an attempt of another admitted key. A should acquire/release the existing slot on each owner call; B must not silently turn the larger quantum into blocking lock acquisition. Required source/catalog/lease waits remain owned by those components. No finite row budget by itself proves a response-time bound. [S2–S3, S11–S12]

The complete journal cost is still the sum over all actual Core attempts, Native phases, retries, warm/cold restorations and required preparation. Reducing two stamps per old turn to two per group changes a coefficient; it does not establish total linear growth. Nor does grouping cure repeated whole-resource work. Keep at least these dimensions distinct:

- Number of finite journals/attempts and all actually attempted turns, including interruption/recovery.
- Actual bytes and frames scanned/restored; complete control widths and catalog/workspace populations.
- Epoch/topology/union/rank work and every repeated fresh dependency check.
- Effective assertion work and graph adjacency per correction, including retired/reseeded state and whole-projection revalidation.
- Resource lifetimes and cumulative opens/closes/transactions, separately from peak simultaneous tickets.

The retained three tiny histories had 107 / 125 / 247 Native advances. One correction replaced one assertion but revalidated all six effective assertions; twelve non-None graph-retirement plans had zero adjacency visits. That supports keeping correction-wide work as an independent concern, not claiming a nontrivial DAG bound from this fixture. Repeated current-budget setup was about 1% of the earlier instrumented advance CPU, and the newer d4d7 codec reuse changes decoding within fresh observations while preserving all 479 recorded noncodec/control traces. None of this measures grouped Core performance. [E1–E2]

Do not attribute repeated catalog construction to every step: the installed admission catalog is already constructed through `lru_cache(maxsize=1)`, and its checkpoint digest is constructed once and returned by `checkpoint_sha256()`. That is distinct from fresh source/preparation/certificate checks and Native effective-assertion work. [S14]

## 6. Required owning decisions and evidence before implementation

| Decision/obligation | Source owner | Necessary evidence |
|---|---|---|
| Quantum, aggregate admission, default and configuration | Core service/runtime configuration and extent policy | Independently bounded attempts; actual input envelopes; one-attempt parity; no hidden archive cap |
| Internal continue/stop disposition and scheduling boundary | Core service unit / stamped caller / scheduler | Preparation-only progress, cold-open adoption followed by False, terminal publication and finalization timing, bounded other-key opportunity |
| Prerequisite reuse or removal | Epoch/topology/union/rank/replay/capture owners | Dependent-row integrity/currentness proof, including unchanged-manifest row deletion/corruption; otherwise preserve every check |
| Every actual child and inclusive primitive composition | Growth declaration and owning recorder/test implementations | Exact ordered samples through a late failure; no dropped rejected child; no callback/native double count; changing input envelopes |
| Capacity, stale authority, recording failure, cancellation/reply loss, restart | Existing Core/Native lifetime owners | Earlier committed/adopted prefix survives; current exception/receiving authority preserved; no uncontrolled retry |
| Per-step vs coalesced adoption | Core validation owner/jobs and Native storage owners | Multi-step Native-ahead exit matrix before considering B; monotone CAS alone is insufficient |
| Resource custody | Core storage grants, retained slot/construction and Native reader owners | Same original handle/ticket/lease until actual receiving; no free-slot fiction with pending readers |
| Whole-path growth | Owning preparation/correction/caller paths | Separate total-work reasoning; scoped tiny witnesses cannot close G10/G12/G13 |

The maintained single-step tests are useful obligations to extend, not permission to rewrite their expected counts. In particular, the capacity witness’s one-call/unchanged-state assertions explain precisely why partial-progress behavior needs separate tests. The service caller/unit growth helpers contain preparation shortcuts and wide/unrelated controls; they are not automatically a tiny ordinary workflow. No new run is needed to identify the design blockers above. [S9, S13, E1]

## Sources and evidence

All source links below are pinned to d4d7. The companion `core-stepping-growth-inventory.json` records source-file SHA-256 values and the current/proposed count classifications without importing the project.

- **S1 — Core unit and stamped caller:** [collection_uploads.py, service advancement](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L2122-L2180); [unit/stamp declarations](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L9411-L10043).
- **S2 — Core Native owner:** [direct delegates/caps/invariants](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_validation_owner.py#L1422-L1833); [reserve, receiving, open and advance](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_validation_owner.py#L1862-L2123); [nested open delegates](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_validation_owner.py#L1128-L1142).
- **S3 — Scheduler:** [runtime loop](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L2208-L2257); [declaration](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L10046-L10336).
- **S4 — Dependent-row checks:** [epoch](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_epochs.py#L126-L153), [topology](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_topology.py#L84-L110), [union](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_union.py#L244-L270), [rank](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_rank.py#L244-L288), [source read and tail check](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L7983-L8025).
- **S5 — Native-facing source authority:** [_OwnedSource](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_validation_owner.py#L402-L439); [certificate implementation](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_certificates.py); [replay binding preparation](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_replay.py#L73-L89).
- **S6 — Preparation admission:** [epoch](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_epochs.py#L418-L434), [topology](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_topology.py#L277-L289), [union](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_union.py#L756-L773), [rank](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_rank.py#L664-L684).
- **S7 — Native session:** [budget](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/journal_session.py#L66-L75), [delegates/declaration](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/journal_session.py#L293-L666), [advance/progress/result/close](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/journal_session.py#L1513-L1612).
- **S8 — Native entry conductor:** [budget](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/_entry_replay.py#L70-L87), [advance declaration](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/_entry_replay.py#L1998-L2188), [budget reservation](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/_entry_replay.py#L2230-L2254), [child/graph/row/phase dispatch](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/_entry_replay.py#L2465-L2673).
- **S9 — Current capacity obligations:** [owner capacity witness](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/unit/test_upload_journal_validation_advance_growth.py#L1217-L1254).
- **S10 — Worker and catalog adoption:** [validation jobs](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_validation_jobs.py#L463-L541).
- **S11 — Resource/configuration ownership:** [runtime configuration](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/runtime_config.py#L131-L134), [operator document](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/runtime_document.py#L420-L430), [storage grants](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/journal_storage_grants.py), [storage resources](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/journal_storage_resources.py#L20-L77), [operation-ticket completion](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/sqlite-admission/src/sqlite_admission/__init__.py#L267-L270).
- **S12 — Work/extent/growth/coordination policy:** [AGENTS.md](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/AGENTS.md); [extent principles](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/scripts/extent_contract.py#L17-L87).
- **S13 — Maintained observation/composition:** [Core unit observer and actual child handoff](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/unit/test_upload_journal_service_unit_growth.py#L278-L495); [Native child completion and exclusive residual reconciliation](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/unit/test_upload_journal_validation_advance_growth.py#L602-L805).
- **S14 — Existing catalog identity lifetime:** [catalog construction cache](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/provenance_catalog.py#L15-L29); [constructed checkpoint identity](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance-contracts/src/riverhog_provenance_contracts/schemas.py#L65-L85).
- **S15 — Intermediate scheduling is not known to be inert:** [schedule finalization](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L3618-L3667); [readiness predicates](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L7369-L7386).
- **E1 — Supplied receiving/request context:** `/workspace/scratch/21c38f9cc56d/core-stepping-context.json`, retaining issue comments [6887528297](https://github.com/nashspence/riverhog/issues/991#issuecomment-6887528297) and [6889181049](https://github.com/nashspence/riverhog/issues/991#issuecomment-6889181049). These are receiving-owner reports, not fresh verification by this audit.
- **E2 — Earlier retained Native observation:** `/workspace/scratch/21c38f9cc56d/native-phase-observation-r2/run.json`, original SHA-256 `019236fdc3b227d49753b54e79915b62d45cf44e788283d3843411611b33c00b`; derived `/workspace/scratch/21c38f9cc56d/native-phase-cost-summary.json`. The observation ran on 3b47; E1 reports the d4d7 exact-input noncodec parity. No timing comparison or grouped-Core measurement is implied.
