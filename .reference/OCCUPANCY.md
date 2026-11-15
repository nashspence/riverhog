# Ready work and execution capacity: source/evidence handoff

## Conclusion

**The supplied evidence does not demonstrate independently admissible journal work waiting while an authorized execution worker is idle. It also does not disprove such a delay in an application deployment.** The ordinary Core journal route has no submission queue, future or worker pool: it executes the chosen unit synchronously on its caller. Its two retained slots preserve journal/resource custody; they cannot supply a denominator for idle execution capacity.

Source analysis identifies the possible serial scheduling boundaries and the observations needed to attribute a delay. A maintained two-key fixture exists, and a few ordinary scheduler calls could reach both original Native owners. That run would establish actual dispatch/admission order, but would not by itself establish a continuously ready unselected key or an idle executor authorized to run it. No product execution was therefore needed for this reference. No occupancy, queue-delay or throughput number is invented from aggregate counts.

The first receiving step is to choose the actual route and name its execution/admission owner before recording utilization. Preserve the current one-step runtime and per-step catalog adoption. The [owning decision 6889902338](https://github.com/nashspence/riverhog/issues/991#issuecomment-6889902338) already settled that boundary; this packet investigates measurement and responsibility only.

Detailed supporting reads are [OWNERSHIP.md](OWNERSHIP.md), [MEASUREMENT.md](MEASUREMENT.md) and [FEASIBILITY.md](FEASIBILITY.md). [EVIDENCE.md](EVIDENCE.md) records source, report attribution and actual static verification.

## 1. Exact checkpoint and retained evidence

The audited source is **d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e**, tree **98cf298e60e4ba9c178229eae907e164265f6e48**, on the receiving `codex/964-session-owner` / PR #979 history. This reference's sole parent is that source; all product paths are unchanged. The preceding Core-stepping reference **46ace062078d08cd70f508a3f3a7d834a92cbb40** has the same product tree. The earlier 3b47-to-d4d7 prefix refinement changes only the Native checkpoint reader and its owning tests, not the caller/scheduler architecture analyzed here.

The [request 6890015223](https://github.com/nashspence/riverhog/issues/991#issuecomment-6890015223) reports a completed tiny F1/C1/K2/Q1 publication on d4d7: one 65,536-byte member, 36.315 seconds for publication and 49.217 seconds for the complete helper, with original exits/cleanup received and fresh/warm/evicted reader verification after source/upload scratch removal. Its recorded finalization deadline was 120 seconds. Those are receiving-owner results, not new execution or a new end-to-end deadline measurement in this packet.

That report classifies all **465 raised attempts out of 51,811 catalog execute attempts** as `sqlite3.OperationalError`, `SQLITE_BUSY` (5), at parameter-free `BEGIN IMMEDIATE`. The failed-attempt intervals total 18.410 ms wall and 12.768 ms thread CPU, stopping at entry to the SQLAlchemy error callback. They exclude later retries, connection/coordination/rollback and receiving; their sum is not a concurrent critical path. The 302 `producer.finish` and 163 unparented observations are scope buckets, not identified source callers or retry histories.

The supplied raw/profile digests are retained in context:

| Reported artifact | SHA-256 |
| --- | --- |
| Catalog-error diagnostic raw record | `5da872016fae26039c92c53d36ec3281374e71b2b8475281cca72d55b8187390` |
| Publication profile | `bdee7c0313e257933888f0fc6058e8387def8c30c1e5f44c3256897d8bd5ee6a` |

Their bytes and exact diagnostic probe were not supplied with the comment or located in the bounded available-workspace review. These hashes identify reported artifacts; they do not establish independent verification here. The report's SQL identity cannot fill the missing metadata of the older 418 raised attempts. Neither historical request/worker-lifetime counts nor the earlier 479 Native phase observations contain the joint ready/admission/queue/worker/receive evidence needed for this question.

## 2. Four different routes and their owners

| Route | Actual opportunity and ordering | Execution/capacity meaning | Attribution limit |
| --- | --- | --- | --- |
| Ordinary Core scheduler | `process_due_provenance_journal_validations(limit=1)` selects warm/fresh candidates, calls one complete unit at a time, and stops after the first catalog-visible progress, with retirement and finalization opportunities retained | Runs synchronously on the calling thread. The validation owner reserves retained slots and per-key nonblocking execution locks; it does not submit a future | Serial calls identify a scheduler/caller boundary. A spare retained slot is not an idle worker |
| Maintained publication fixture's SDK/coordinator | `_LocalApi.seal...` first gives the direct Core scheduler an opportunity, then issues the selected-journal HTTP seal. The SDK upload helper repeats that journal's seal synchronously until it leaves `validating` | Direct and HTTP opportunities occur in sequence for that invocation. Fixture finalization polls separately drive custody/finalization coordinators with `join_workers=False` | Two opportunities per wrapper are not two concurrent Native workers. The outer `producer.finish` scope can contain several different children |
| Selected-journal HTTP | The synchronous seal endpoint calls `seal_provenance_journal`, then its selected journal's stamped Core unit | The framework runs the synchronous endpoint through its thread machinery. Admission/queue/worker evidence belongs to the actual transport, event loop and limiter | A thread observed at one hook is not a permanently allocated journal worker. Aggregate request/lifetime counts supply no occupancy interval |
| Application lifespan | One archive-maintenance coroutine awaits one `asyncio.to_thread` maintenance call at a time; its full sweep includes one journal scheduler opportunity among other owners | The lifespan also starts retrieval-restore and retrieval-cache tasks. Archive finalization/custody physical workers are a distinct explicitly admitted execution owner | Entering `TestClient` activates this additional scheduling. Idle archive/finalizer capacity is not automatically usable journal-validation capacity |

Source anchors: [Core scheduler and unit](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L2086-L2251); [fixture adapter](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/unit/test_operation_lifecycle_api.py#L234-L289); [SDK journal upload loop](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-client/src/riverhog_client/client.py#L1589-L1642); [HTTP route](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_api/routers/collections.py#L598-L608); [maintenance and lifespan](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_api/app.py#L113-L385).

The maintained fixture constructs unentered TestClient instances. Its exact pinned transport/backend source explains per-request portal/thread lifetimes; it does not supply observed simultaneous capacity. Reusing an entered client would also run application startup/lifespan and its reapers. Such a change is a different scheduling experiment, not a neutral way to remove transport overhead.

The fixture also stages work through synchronous dependency order: individual journal upload/seal operations complete before the surrounding loop proceeds, and later history bindings can require already obtained anchors. Serial construction of the next journal is different from delaying a ready, admitted journal. The two-key unit-test fixture provides a separate, genuinely independent setup; it must not be confused with a claim that the ordinary publication fixture already exposes two ready keys at the same time.

## 3. What a fresh ready/admissible key can mean

An observable identity must distinguish the exact collection/journal/attempt, captured source directory/version/digest/bytes/cursor, source/catalog binding, and—when selected—the workspace, certificate binding, job and worker epoch. A label such as `journal A` is only a trace-local reference to that exact descriptor. A digest or retained descriptor identifies what was selected; it is never an authority cache.

The following stages have different evidential meaning:

| Stage | Observable fact | What it does not prove |
| --- | --- | --- |
| Candidate selected | A selector or warm-slot inventory returned a key | Complete preparation, fresh authority, a free execution slot, or available worker |
| Core ready to attempt | The actual selected unit completed its current source/capture, epoch, topology, union, rank and replay checks in owning order before calling the owner | A single atomic snapshot or a readiness lifetime extending through a later wait; slot/lease/worker/Native resource admission still follows |
| Slot reservation | The actual owner reserved the exact key and acquired its nonblocking per-key lock | Successful workspace lease/worker claim, current certificate, SQLite capacity or Native semantic progress |
| Workspace/worker ownership | The existing path obtained the exact lease and conditionally selected/claimed its current worker | Permission to reuse old source/job checks or create a second owner after lost acknowledgment |
| Native invocation entered | The actual `JournalValidationSession.advance` call began | All its internal source/ledger/budget/SQLite admissions succeeded; those checks still execute inside the call |
| Progress committed/adopted | A Native transaction or the independent catalog publication actually completed under its fences | Physical worker exit, semantic receiving or final resource closure |
| Original received | The owning receiver confirms the exact worker/future/result and/or resource close appropriate to that original | Global quiescence or the release of a separately retained reader/lease |

The ordinary Core guard chain consists of separate fresh operations. Its last return cannot retroactively turn them into one atomic eligibility snapshot. Conversely, the observer must not add substitute readiness queries or skip preparation merely to manufacture such a point. Record the actual chain's outcome and exact source; leave unsupported intervals unknown.

The existing `ValidationAdvance(False)` is especially unsuitable as a reason code. It can reflect busy/full reservation, source/lease/currentness rejection, capacity or other distinct paths; cold opening may already have adopted recovered progress. Actual stamp differences, branch-specific admission/refusal outcomes and original receiving are necessary to classify it. [Owner control flow](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_validation_owner.py#L1862-L2122).

### Required evidence for an idle-capacity conclusion

For a nonempty interval, the trace would need to establish all of the following for one exact independently eligible key:

1. Its readiness/admission authority remains valid for that interval under an owning lifetime or complete invalidation evidence; a prior true observation is insufficient.
2. The key is actually offered to the relevant scheduling boundary, with any dependency ordering and permission/resource grants recorded.
3. A specifically identified execution owner has usable capacity authorized for that operation throughout the interval. This excludes slots retained by unknown originals, unreceived results/readers, closed gates, other occupied grants and unrelated worker pools.
4. The key is not executing, and busy-key, writer/resource admission and other required dependencies do not explain the gap.
5. The scheduling owner can identify the remaining disposition. Only then can the interval support a narrow claim of avoidable serialization; a potential unexplained gap is not automatically a throughput opportunity.

The current aggregate reports do not establish this conjunction. A source-derived serial call chain establishes ordering, not that the work on either side was independently admissible while idle authorized execution capacity existed.

## 4. A bounded observation and offline reconciliation contract

This is a proposed diagnostic contract, not a shipped observer, accepted product interface or execution claim. The detailed [measurement analysis](MEASUREMENT.md) names source boundaries and accounting limits.

**Identity and clock.** Use one monotonic clock domain with an origin, integer nanosecond timestamps, a lossless sequence, and trace-local operation/key/original/worker/future/resource identifiers. Keep parent operations distinct from worker execution and receiving. Cross-thread equal timestamps require explicit causality IDs; wall-clock labels do not establish ordering. Thread identities may be reused, so allocate original-lifetime tokens instead of treating an OS thread ID as permanent identity.

**Actual event families.** Preserve source selection and exact descriptor; each actual guard-chain outcome; slot/lease/worker admission or refusal; method entry and return/BaseException; Native phase/commit and catalog adoption where observed; actual queue registration, physical start/exit, future settlement, join, semantic receiving and resource-release receipts for routes that possess them. Missing event families remain absent/unknown. Do not fabricate submission or join records for the synchronous Core route.

**Submission order.** A worker may start or finish before `try_submit` returns. A wrapper around method return cannot supply the admission timestamp. A true admission-to-start interval requires a source-owned marker after the actual grant/registration and before physical start. An outer invocation-to-entry interval includes admission overhead and must retain that different name and meaning.

**Refusal identity.** Record the exact source-owned callsite and disposition, the relevant original, and an approved exception type/database-code/parameter-free statement identifier. Do not record SQL parameter values, exception messages, connection URLs or private deployment identities. An outer scope such as `producer.finish` remains useful parent context, never a substitute for the actual caller.

**Lossless finite storage.** Select a finite key/turn scope before effects, derive a record/byte envelope from independently bounded actual event multiplicities, and reserve the original-operation tail needed for exits and receiving before admitting a call. Account for full identity/metadata widths and observer work. Stop admitting new observations before exhausting the envelope. Silent ring overwrite, dropped events, sampled admissions or a guessed cleanup receipt cannot produce a complete trace. If no adequate event/tail bound is available, establish that instrumentation boundary before executing.

**Semantic transparency.** Wrappers must preserve original return values, exception objects and `BaseException` propagation, generator laziness/close behavior, transaction boundaries and original cleanup. Recording must not add SQL, poll a readiness predicate, hold catalog readers across Native work, wait while holding owner locks, create a new worker or gate a maintained worker to obtain a desired schedule. Existing deliberately gated custody tests establish lifecycle behavior, not ordinary queue delay.

An offline reconciler would:

1. Validate exact source/probe/dependency pins, record limits, sequence/clock domains and referenced identities before computing intervals.
2. Match each invocation and original to its actual entry, exit, outcome, physical receiving and resource receiving; preserve unpaired tails as partial rather than infer completion.
3. Reconstruct admission and occupancy separately for each actual execution owner. A completed future is not a joined worker; joined execution is not necessarily semantic/resource receiving.
4. Compute queue/admission/start/receive intervals only where both boundaries have the required ownership and clock evidence. For synchronous calls, report their actual execution/receiving intervals without inventing a queue.
5. Intersect only proved ready lifetimes with proved available authorized-capacity intervals, excluding known dependency/refusal/unknown-original states. Missing data yields an explicit unknown, never zero waiting or zero idle capacity.
6. Reconcile every child and refusal once, and keep overlapping inclusive intervals separate from elapsed critical-path measurements. Sum CPU work only in its valid metric domain; it is not worker occupancy.

No actual queue/worker intervals were collected in this task, so there is no executed trace or runtime interval-reconciler result to supply. The packet contains the static boundary/event design, exact source evidence and raw supplied comment bodies. No synthetic event stream is presented as observation.

## 5. The legal tiny fixture and why it was not run

The maintained [two-journal scheduler test](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/unit/test_upload_journal_validation_owner.py#L324-L369) seeds distinct journal IDs with independent tiny source bytes in one catalog, begins capture through the owning path and drives the ordinary scheduler. It does not fabricate completed preparation. Its existing completion loop permits up to 1,000 iterations; running that whole test would exceed the focused few-turn scope.

For its one-initial-entry/no-relation source, static analysis supports a possible successful-first-candidate path in which the ordinary capture, epoch and empty topology/union/rank preparations fit their existing per-call limits. Two scheduler calls can then enter different owners; two additional calls can revisit them with all fresh checks. This is a conditional source-derived path, not an observed timing or promise that every call reaches Native.

Four `process_due_provenance_journal_validations(limit=1)` calls are **four scheduler invocations**, not an unconditional four-child bound. With configured retained capacity C and invocation count L, the existing warm/fresh selection structure gives the conservative candidate/Core-unit/Native-attempt cap **L(2C+1)**. At C=2 and L=4 that cap is 20. A proven closed two-key universe permits a tighter candidate argument, but every actual preparation-only, refused or non-progressing child still counts.

Each attempted unit preserves the ordinary capture and Native budgets, source/preparation checks, Native phases and durable commits, per-step catalog adoption, scheduler cursor/retirement behavior and finalization opportunities. A private `_advance` shortcut would omit the surrounding scheduler behavior and is not the proposed experiment.

That prefix could validate event transparency and actual key dispatch order. It cannot alone establish the missing ready lifetime or an idle authorized executor, because the direct Core route owns neither a queued ready-work grant nor a worker pool. Adding concurrent HTTP callers or entering TestClient would introduce a distinct scheduling condition requiring its own observation ownership; it would not answer the ordinary-fixture question by substitution. Static analysis is therefore sufficient for the narrow negative evidence result here, and no fixture/probe was started.

If a later owner chooses such a prefix as an instrumentation check, it must use one approximately 120-second cooperative workload cap covering setup and actual turns, with reserved receiving capacity. Stop new admission at the boundary, receive every entered original and remove scratch only after confirmed receiving. A deadline or cancellation request is not a physical/semantic receipt. If an entered operation outlasts the admission window, preserve the partial trace and actual eventual exits instead of reporting a completed two-minute run.

## 6. Catalog writer refusals: narrow source attribution

SQLite's catalog transaction hook selects `BEGIN IMMEDIATE` for an early writer. With explicit nonwaiting deferral, it temporarily sets `busy_timeout=0`, restores the previous value in `finally`, and lets the actual error propagate to the owning caller. The shared contention classifier recognizes the SQLite busy/locked family and selected PostgreSQL serialization/lock codes; it does not provide an operation outcome or resource-closure receipt. [Catalog boundary](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/catalog_db.py#L75-L133).

The following are relevant source-owned candidates, not a census or assignments of the reported 302/163 attempts:

| Candidate family | Refusal disposition in source | Consequence for measurement |
| --- | --- | --- |
| Schedule-ready finalizations | A classified writer refusal is caught by `process_due_finalizations`, which returns progress already received before that attempt | One outer successful/no-progress poll may contain a refused scheduling transaction |
| Finalizer/custody registration | `ArchiveFinalizerWork._submit` can return `busy` before constructing/running a new original | Distinguish local registration/writer refusal from an idle or busy execution worker |
| Original finalization admission | The retained physical pipeline waits for the registering transaction to exit, then resolves its exact claim and gate; a failed transaction does not cache a no-effect decision | A physically entered worker can still be resolving authority, not executing the semantic journal body |
| Maintenance/retirement and terminal fences | Selected paths defer by returning prior progress, `False` or an opted-in `busy`; exact receiving remains with the original | A refusal can occur during receiving/retirement rather than dispatch |
| Retained failure feedback | A busy feedback transaction may be omitted while the original semantic exception remains primary and is re-raised | Error-callback timing does not include subsequent feedback or original receiving |

[OWNERSHIP.md](OWNERSHIP.md) pins the exact callsites, catches and receiver paths. The parameter-free statement and database code are shared by these paths. To attribute the new 465 attempts, correlate each attempt ID to the exact method/callsite, parent/original, classified disposition and eventual receiving. The supplied report lacks that mapping. In particular, unparented does not mean background worker, and `producer.finish` does not mean the SDK owns the underlying retry.

## 7. Work, resources and practical limits

The measurement must preserve independent growth obligations. A small event count cannot erase the actual work of capture/preparation, full metadata comparisons/codecs, indexed populations, Native phases, catalog coordination, source bytes, callbacks or receiving. Record actual child multiplicities and each child's valid inputs; compose exclusive parent work plus inclusive non-overlapping children in their existing domains. Nested Native/provider/Core intervals cannot be summed as independent costs.

Peak retained resources and cumulative work have separate meanings. A journal slot may be warm, executing, closing, awaiting an independent reader or retaining an unknown close outcome. Zero active Native calls does not release those slots. Original construction/storage/connection/lease custody remains charged until its receiver confirms, including after cancellation or reply loss. The distinct physical execution owner releases its physical slot after actual worker exit, join and collection of the done future; later semantic feedback/disposal can still retain separate construction, result or reader resources. Preserve those separate capacities and lifetimes. Neither a free physical slot nor an idle thread automatically becomes a journal grant. [Physical receiving and slot release](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/physical_execution.py#L348-L380).

The observer also has costs: exact-identity formatting/hashing, timestamps, event serialization and buffers must be bounded and disclosed. These costs perturb the measured lane. Even a complete finite trace would describe the chosen diagnostic conditions, not production throughput, a universal scheduling bound, total memory, whole-path non-growth clearance or correction-wide scaling. Correction revalidation, transaction cost and transport/lifespan overhead remain separate questions.

## 8. Owning responsibility and disposition

The source locates serial ordering in the caller/scheduler path: direct Core candidate execution; the fixture's direct scheduler then HTTP sequence; the SDK's per-journal sealing loop; and the lifespan's serialized maintenance turn. Admission and original receiving remain with their respective journal, archive, transport and resource owners. None of those source facts alone establishes a ready-key/idle-worker overlap.

The focused next receiving action is to select one route and require its source-owned readiness/admission lifetime plus its actual execution-owner markers. For the two catalog-refusal buckets, first use the retained raw diagnostic if the owner has it to determine whether those callsite/disposition correlations were recorded; aggregate scope counts cannot reconstruct them. A later bounded observation should be commissioned only for a remaining question its available events can answer.

**No runtime optimization is proposed.** The accepted one-step/per-step-adoption decision remains. This reference does not reopen grouping, skip guards, fuse Native phases, remove commits, change concurrency or start a publication/census campaign. The result is an explicit absence of the required evidence, with concrete source ownership and measurement gaps—not a finding that useful parallelism can never exist. #991, whole-path #964 and G10/G12/G13 remain open under their existing authority.
