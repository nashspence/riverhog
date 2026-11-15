# Bounded Core stepping: authority, adoption and receiving

## Conclusion and evidence boundary

The current sources support a limited positive finding: several **unchanged** Native advances can share a finite scheduling admission without fusing Native phases or transactions. That does not establish that the surrounding Core checks can be performed only once. The strongest concrete obstacle is that the service readiness path validates retained dependency rows which Native's source/worker/certificate callbacks do not validate. A loop which skips that path can therefore perform more Native work after corruption that the next existing Core unit would reject first.

If further implementation is justified, prefer **A: bounded calls to the existing single-step `UploadJournalValidationOwner.advance`, with catalog adoption after every call**. This preserves the established owner error, CAS, terminal and receiving behavior. It still requires fresh readiness validation at every former unit boundary, or a source-owner proof for an equivalent consolidated validator. **B: several Native calls followed by one catalog adoption should remain out of the initial change.** It adds partial-progress, conflict-detection and recovery decisions without resolving the readiness obstacle. No runtime change is demonstrated or recommended for immediate adoption by this static packet.

This audit inspected clean `d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e`, its exact delta from `3b47cbba770c31eb1487b50f7e3b16533c10e827`, and existing tests. It executed no product calls, tests, fixtures or probes. References below are immutable source links at d4d7. Counterexamples are source-derived interleavings, not newly executed failures.

The supplied later comment 6889181049 states that d4d7 is fully qualified and that all 418 historical catalog raises occurred during finalization; it still does not identify their exception types, driver codes, statement families or retry identity. This audit consumes that supplied update, rather than re-verifying CI or reclassifying the raises. It makes no throughput, historical cost, whole-path G10/G12/G13 or concurrency-utilization claim.

## 1. What the current unit actually owns

### Service selection and readiness

The stamped service entry reads a catalog stamp, executes one unit, then compares a fresh stamp. The stamp includes journal state/capture version, preparation scan version and validation-job version. It is an observation of catalog change, not a count of Native calls or a proof that no work happened when it stays equal. [S1]

The unit can first execute a bounded capture action. It then reconstructs the exact validation work, checks the captured directory and last captured page, and calls epoch scan, topology, union, rank and replay preparation in order. A not-ready result returns before Native. Once these are ready, it invokes the owner once and conditionally publishes a terminal journal outcome. Completed preparation calls still validate metadata and retained links; they are not mere Boolean cache hits. Epoch readiness also calls bounded retired-preparation reclamation before returning ready. [S1, S2, S3]

The work identity includes collection/journal, attempt, captured directory, capture version, exact source bytes and SHA-256, capture phase, cursor positions/count and failure. The source fence compares all of those against a journal still in `validating` state. `_read_upload_journal_validation` additionally checks the directory and hashes the last captured page; the fence alone does not do that page verification. [S2]

### Retained owner and opening

The owner pool is keyed by `(collection_id, journal_id, attempt_id)`. Its mutex protects reservations only. Each exact slot has a nonblocking execution lock; a busy key or full pool returns without waiting for Native work. The default two retained slots are capacity, not an executor. [S4]

Opening creates/retains the original passive `JournalConstruction` before effects, constructs exact source/certificate bindings, selects a job, acquires the exact workspace kernel lease, and only then claims a new worker epoch. Active accepted work never recreates a missing database. Native opening restores or initializes the ledger under that worker epoch. The slot receives the early storage handle even if construction fails; successful opening transfers the workspace lease lifetime to final Native storage. `_open` immediately adopts restored Native progress before executing the ordinary next advance. [S5, S6]

`_OwnedSource` intentionally retains the originally claimed `ValidationJob`. `worker_current` compares the durable workspace, collection/journal, attempt, both bindings and worker epoch, plus an allowed active/opening/complete state. It does **not** compare that saved job's version or progress. The separate publication operation compares the latest slot job's version and previous progress. Updating the saved source job after every adoption, or using its old progress as a publication CAS, would conflate distinct responsibilities. [S7, S8]

### One Native call and catalog adoption

An ordinary owner call rechecks the exact work/catalog identity, calls `current.progress()`, performs one `current.advance(budget)`, and publishes the resulting progress. A semantic `ProvenanceValidationError`/`ValueError` causes a fresh progress read and a failed outcome. Successful completion borrows the original result, checks the exact journal ID/bytes/SHA-256 and terminal anchor, closes that borrowed summary, adopts the outcome, and then drops the owner slot. [S9]

Every unchanged `Session.advance` checks the exact source binding and `_OwnedSource.verify_current` before its Native ledger load and action. That callback checks the worker, exact replay source binding and certificate manifest through fresh catalog snapshots. Source reads repeat their own authority/range checks. Native load, transactional fence and publication remain fresh and each ordinary Native transaction advances exactly one checkpoint version. The d4d7 codec refinement reuses parsed prefix state only inside each fresh read; it does not introduce a cross-call checkpoint or acceptance cache. [S7, S10, S11]

`jobs.publish_progress` deliberately accepts a monotonic jump in Native version: it rejects backward version/frame/octet/source progress, reversal of completion and a changed accepted prefix at an unchanged frame count. Publication additionally requires the exact source fence and job identity, latest catalog job version, previous encoded progress and opening/active state. Thus a jump is representable. That fact alone does not decide when a multi-step unit must publish it. [S8]

## 2. A and B are different designs

| Property | A: several existing owner calls, per-step adoption | B: several Native calls, final adoption |
| --- | --- | --- |
| Native state machine and commits | Unchanged | Can remain unchanged |
| Worker/source/certificate checks | Existing owner and Native paths repeat | Native callbacks repeat; any skipped owner checks must be specified |
| Job version/progress CAS | Existing CAS after each step | One final CAS; intervening version-only conflict is detected later |
| Capacity after earlier progress | Earlier calls are already adopted; stop on the paused call | Existing catch returns `False` without adopting earlier calls; needs a new rule |
| Semantic failure | Existing failed-outcome path adopts and closes at that step | Must select fresh progress and publish failure before terminating the group |
| Terminal result | Existing borrow/check/adopt/drop path | Must run exactly once and stop before another inner call |
| Generic exception or cancellation | Existing failing owner call handles its original slot; previous progress is adopted | Several committed turns may remain ahead; cleanup/reopen rules must cover the larger interval |
| Readiness checks skipped outside owner | Not justified by A itself | Not justified by B either |
| Pool sweeps/reservations | Repeat and must be counted | May be amortized under a retained slot lock; changes lock-holding and cleanup timing |
| Initial proof burden | Smaller, but still a new scheduling/growth envelope | Larger, with new publication and partial-result semantics |

A service-level A loop releases the slot lock after each existing owner call. Another same-key caller may acquire it before the next iteration; the next call must reserve and read the latest state normally. B would normally retain that lock across the group. Either design must leave the pool mutex outside Native/catalog/filesystem work and must not add a catalog transaction spanning multiple Native commits. [S4, S9]

## 3. Concrete counterexamples and classification

### C1. An unchanged manifest does not prove its dependency rows still exist

After one successful Native turn, remove or change a retained preparation dependency without changing the Scan row, source or worker identity. The next existing service unit checks these links before entering Native:

| Owner | Fresh check on an already completed preparation |
| --- | --- |
| Epoch scan | The last published Frame has the expected attempt, epoch, reference metadata and ending byte position. |
| Topology | `header.last` still names a Fact with the expected directory and digest. |
| Union | The last source Fact and last published Arc still exist with their expected identity. |
| Rank | The last vertex, arc and graph result still match the checkpoint's retained markers/counters. |
| Captured source | The directory and last captured page still agree with the checkpoint, including page digest. |

These checks are in [S2, S3]. Certificate `verify_current` compares the Scan manifest fields and source fence; it does not join those dependency tables. Source `_probe` returns the exact source/replay-binding metadata; it does not validate those rows either. A proposed inner non-graph Native turn can therefore pass its source/worker/certificate checks after, for example, a deleted last rank graph result that `rank._begin` would reject. [S7, S12]

Existing tests deliberately corrupt these links and demand rejection. The rank graph-marker case is particularly direct: it finishes rank preparation, deletes the last recorded graph Result without changing the Scan checkpoint, and expects the next readiness call to fail. These tests were inspected, not run. [T4]

This is a blocker to claiming that bare per-step callbacks are equivalent to the previous readiness boundary. It is not proof that every imaginable admitted lifetime is impossible. The Core preparation owners may define a different lifetime only with an explicit immutability/custody argument or an equivalent fresh validation path. A version check over the Scan header alone is insufficient for the current corruption behavior.

### C2. Earlier progress followed by capacity is not zero progress

Consider a warm slot with catalog progress at Native version v. Inner step 1 commits v+1 and returns; inner step 2 raises `JournalValidationCapacity`. Moving the existing capacity handler around that loop returns `ValidationAdvance(False)` without adopting v+1. With no opening or unrelated catalog change, the outer stamp also remains equal even though Native advanced. Retrying the same inadequate admission may immediately pause again, leaving the committed progress unreported until a successful adoption or reopen. This is a source-derived reporting/liveness difference, not evidence of lost Native bytes. [S1, S9]

A avoids the new gap because step 1 used the unchanged owner path and already adopted its result. B needs a deliberate choice: adopt a safely selected current snapshot on a capacity exit, or explicitly expose retained Native-ahead progress and recovery responsibility. Blindly publishing a remembered result in a `finally` block is unsafe: the source/worker may have become stale, the failed call may have committed before a later failure, or cleanup may already have begun. `JournalValidationCapacity`, `SqliteCapacity`, semantic failure and unknown exceptions must retain their existing distinct handling rather than being treated as one resumable outcome.

There is also an existing nuance for A: an owner call returning `progressed=False` can have successfully opened and adopted initial/restored progress before encountering capacity. The current outer stamp detects this. A group must preserve catalog change observation; it cannot replace it with “all child flags were false, therefore nothing changed.” [S5, S9]

### C3. A worker-current predicate is not a publication CAS

If another actor changes only `Job.version` or progress while leaving identity/epoch/state unchanged, `worker_current` can remain true. The existing owner rejects the stale publication CAS after its next Native call. B can execute more Native calls before reaching its single final CAS. It must never overwrite the changed job or quietly retry the CAS using a newly fetched job as if that conferred authority over the earlier work. This is an additional detection-delay/extra-work decision for B, even though the final CAS prevents an incorrect catalog overwrite. [S8, T1]

Source replacement, worker-epoch replacement and a changed certificate manifest are different: unchanged Native entry checks reject them on the next inner step. A manifest replacement during an unopened attempt is treated as stale selection; replacement after ownership has opened can become a lost retained workspace. Preserve that distinction. [S7, S9, T1]

### C4. Terminal publication can succeed while receiving fails

The owner writes the complete/failed job outcome before `_drop`. If closing the original Native storage raises, the slot is marked closing, retained capacity and lease remain charged, and the confirmed outcome remains in the job. A subsequent call must first receive that same original; it must not rerun validation, erase the successful job, replace the slot or convert the cleanup failure to a pause/success. [S4, S9, T2]

Independent borrowed summaries can keep storage and its original construction/lease alive after the Session facade closes. `_drop` cannot delete the slot until construction, storage and workspace receiving confirm. Unknown Native close outcomes cannot be inferred from a matching pathname, descriptor identity, elapsed time or a returned HTTP cancellation. [S4, S6, S13, T2]

### C5. Continuing after a terminal step changes both work and error behavior

After the first terminal outcome, the original owner may already have closed/dropped the Session. Re-entering either a remembered Session or another owner call can fail, reopen terminal metadata, or perform extra source observations. The group must stop immediately at the first success/failure outcome and perform the existing final source-fenced journal publication once. A successfully adopted outcome whose reply was lost remains recoverable through `jobs.outcome`; terminal journal publication is independently fenced against duplicate or stale updates. [S5, S9, S14, T1, T3]

## 4. Exit, interference and recovery matrix

| Event | Required behavior and existing recovery boundary |
| --- | --- |
| Step/work admission exhausted | Stop before another attempted child. Preserve the latest adopted checkpoint; a larger future admission is a new unit. This is a per-unit limit, never an archive-size cap. |
| Busy same key or full owner pool | Return without waiting or opening a replacement; a scheduler may select another candidate. Retained closing/reader-held slots still consume capacity. |
| Native capacity pause | Do not busy-retry inside the group with unchanged admission. Keep the same usable warm atom where the existing path does; partial initialization still receives its early handle. A has already adopted earlier successful calls. |
| Semantic validation failure | Preserve the first actual failure, obtain the existing current-progress observation, publish the failed job under source/job CAS, and stop. Do not continue to later document errors. |
| Stale source/attempt/worker | Stop on the first fresh refusal. No failure or success for the old work may overwrite the replacement. Retire/receive the original slot without deleting recovery authority. |
| Invalid certificate, lost workspace or Native database error | Preserve the existing lost-workspace classification and best-effort fenced lost-state publication, then receive/drop the original and propagate the owning error. Do not fabricate successful progress. |
| Catalog publication CAS loses | Stop; do not refetch a new job and overwrite it. Native may be ahead, but the stale worker's detached result is not current authority. |
| Catalog transaction raises or reply is uncertain | Preserve the original exception/unknown outcome. Do not classify a historical catalog raise as capacity or retryable contention without its actual class/code. Re-entry uses the current durable job and workspace. |
| Caller cancellation while synchronous work is entered | A cancellation request is not proof that the body stopped. Preserve original executor and resource receiving; do not free the slot/lease early. Check any explicit cooperative cancellation only between completed child calls, with its outcome contract defined by the caller. |
| Source deletion from upload cancellation | The journal source fence becomes false. The job's foreign key uses `SET NULL`, retaining the workspace cleanup identity. No further old-source acceptance or replacement workspace creation is permitted. |
| Process exit after Native commit, before adoption | Kernel lease release permits a later worker to claim a higher epoch, reopen the same database, validate/replace the Native worker, and adopt its committed prefix before more work. Never recreate an active missing file. |
| Reply loss after job outcome adoption, before journal publication | A later owner reads the persisted terminal outcome, and the service performs exact terminal publication. No semantic replay is necessary. |
| Reply loss after terminal journal publication | A repeated seal sees the terminal journal state. The existing conditional publication prevents applying the same detached outcome to a changed attempt. |
| Final independent reader still retained | Keep the exact original Session/storage/construction/lease and owner slot charged. Other admitted keys may advance; this key cannot be force-received. |

The current real-process test interrupts immediately before catalog publication of an accepted Native prefix and proves recovery without rereading that accepted frame. That supports the architectural Native-ahead mechanism. It does not by itself qualify k-step lag, a later capacity exit or cancellation within a proposed group. The original test assertions and process behavior were inspected only. [T1]

There is no cooperative cancellation parameter in the current owner `advance` API. A new multi-step admission cannot claim per-step cancellation responsiveness merely because its loop is finite. Nor does a count cap give a hard wall-time cap for an entered synchronous database or filesystem call. Service close sets the owner closed and waits for actual slot-lock release before receiving it. [S4, S9]

## 5. Smallest defensible future protocol

These are design requirements, not a new public API or an implemented contract.

1. **Explicit admission before effects.** Keep the current one-turn behavior as the default (`K=1`). The Core scheduling owner selects a positive finite maximum of attempted Core turns and a per-step Native budget. Every attempted turn, including preparation-only work, capacity and failure, consumes the allowance. The resource owner retains the original task and grants for the entire body and subsequent receiving. Native's existing row/source/work budget semantics remain unchanged; a sequence of k full budgets costs up to k budgets, not one budget.
2. **Fresh exact readiness before each admitted step.** Preserve source reconstruction, directory/page integrity, preparation dependency checks, cleanup readiness and binding checks in their owning order. An equivalent consolidated fresh validator could reduce duplicate reads inside one boundary, but it must account for every old predicate and its failure behavior. Do not infer readiness from a previous success, header equality or the absence of graph work in the next Native phase.
3. **Use the existing owner operation.** Invoke `owner.advance` once per attempted step, with its original reservation, fresh progress/source checks, Native action, catalog CAS and cleanup. Keep catalog snapshots closed before Native computation and the pool mutex outside work.
4. **Stop at the first boundary result.** Stop on `False`, terminal outcome, exception, exhausted allowance, observed cancellation, or new lack of admission. Do not spin through capacity or cleanup. Previous completed steps retain their own observations and adoptions even when a later step fails.
5. **Keep terminal publication separate.** The first terminal outcome follows the existing fixed-shape exact-source journal publication. Do not invent a group outcome that bypasses the Native result borrow or source-fenced seal.
6. **Keep progress observation honest.** The service may compare its original and final catalog stamps for the group, while retaining actual child counts and intermediate outcomes for evidence. It must not interpret a final child `False` as erasing earlier changes, or claim that unchanged catalog metadata means Native did no work.
7. **Yield to scheduling after the finite group.** Preserve the scheduler's cursor rotation, candidate selection and retirement turn. The next group is a new admission. Several Native phases within one key remain sequential; no extra worker or concurrency setting is implied.

The lowest-change alternative is to keep the current single-step unit until those ownership decisions are settled. A loop around complete existing units could preserve more checks while amortizing only the outer stamps, but it also needs an explicit per-iteration status and a growth/resource envelope: the current unit returns `None`, and a not-ready preparation can still have committed bounded work. It is not a safe drop-in “repeat until progress” shortcut. [S1]

## 6. Fairness, growth and custody consequences

The existing scheduler tries candidate keys in a rotated order, stops after one progressed candidate per requested unit, and alternates retirement opportunities. A multi-step unit increases how much selected-key work can precede the next candidate opportunity. A finite count bounds attempted turns, not elapsed waiting, and unchanged concurrency does not preserve the previous fairness quantum automatically. The source owner must choose the maximum admitted work per scheduling unit and demonstrate that an independently ready key and cleanup still get turns. [S15]

No catalog snapshot/transaction should be held across the group. Existing Native work deliberately runs after catalog readers close, allowing unrelated catalog updates and source replacement. A transaction spanning the group would change lock scope, currentness visibility, cancellation and cross-database commit behavior. The handoff test checks this independence while Native is paused. [T3]

Do not add k retained results, copies of workspaces, source buffers or child receipts without explicit bounded ownership. The same slot can remain warm; result borrowing should remain limited to the terminal operation. Original SQLite construction grants, file records, workspace lease and independent-reader obligations persist until confirmed receiving. Count every completed Native child even if no enclosing successful group result returns. [S6, S13]

Reducing outer invocation counts does not remove Native per-phase commits, correction-wide retirement/reseed/revalidation, preparation work, graph work or primitive SQL/source costs. The total/inclusive growth composition must sum actual children and changing argument dimensions. The separate growth audit owns the detailed multiplicity inventory; this lifetime audit supplies no whole-stream or archive-size bound.

## 7. Existing tests inspected, and what remains unproved

| Inspected source tests | Existing obligation they exercise |
| --- | --- |
| Owner outcomes, capacity, missing accepted database, kernel lease competition, real process exit before adoption, stale CAS, changed rank manifest, independent journals. [T1] | Retained single-step ownership and recovery, not grouped-turn equivalence. |
| Unconfirmed close, failed opening, confirmed terminal outcome with cleanup failure, capacity during initialization, independent final readers and independent slot progress. [T2] | Original receiving and charged capacity through partial/terminal cleanup. |
| Exact detached source reads, stale success/failure, duplicate terminal publication, unrelated catalog work during Native, deleted capture manifest link and process exit around terminal commit. [T3] | Source and publication fences, no catalog reader held over Native, durable terminal boundaries. |
| Missing/corrupt epoch, topology, union and rank links, plus captured page corruption. [T4] | The readiness predicates that bare inner Native callbacks do not subsume. |
| New warm-prefix corruption cases at d4d7. [T5] | Fresh canonical prefix checks on load/fence/publication despite within-invocation codec reuse. |

None was run by this audit. The missing grouped-boundary evidence includes late capacity after successful inner calls, source/worker/certificate or dependency mutation between calls, final adoption loss, terminal cleanup failure, reply loss, cancellation and two-key fairness under the explicit larger quantum. Their absence supports keeping B and any omitted-readiness design out, rather than assuming a small successful timing observation would prove them.

## Source references

- [S1](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L2122-L2177): stamped service unit and preparation order. Stamp fields: same file, lines 7874–7902.
- [S2](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L7931-L8026): exact work, source fence, validation selection and tail-page check. Page validation: lines 8131–8162.
- [S3](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_epochs.py#L71-L153): epoch retained links and readiness, also lines 418–433. Topology: `upload_journal_topology.py` 54–108, 277–289; union: `upload_journal_union.py` 212–268, 756–773; rank: `upload_journal_rank.py` 218–291, 664–683; cleanup: `upload_journal_cleanup.py` 288–298.
- [S4](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_validation_owner.py#L1836-L1953): pool reservation, slot drop, stale sweep and close.
- [S5](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_validation_owner.py#L1955-L2027): source/job selection, lease, worker claim, retained opening and initial adoption.
- [S6](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/journal_validation_workspace.py#L329-L426): exact workspace selection and lease receiving contract. `journal_validation_workspace_lease.py` 79–159: original lease lifetime. Native `journal_session.py` 1325–1445: early retained storage and worker replacement.
- [S7](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_validation_owner.py#L402-L438): `_OwnedSource` callbacks. `upload_journal_source.py` 362–458: binding probe, per-read currentness and page handling.
- [S8](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_validation_jobs.py#L351-L544): job identity, worker claiming/currentness and progress publication. Terminal outcome codec: lines 547–566. Retirement: lines 592–603.
- [S9](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_validation_owner.py#L2029-L2122): complete single-step owner control and exception order.
- [S10](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/journal_session.py#L1456-L1595): Native transactions, terminal validation, advance, progress and result borrowing.
- [S11](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/_session_checkpoint.py#L416-L566): fresh prefix observation, load/fence and independent publication parse.
- [S12](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_certificates.py#L660-L751): certificate manifest construction and fresh point check. Manifest fields: lines 43–62. Replay source point probe: `upload_journal_replay.py` 181–212.
- [S13](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/journal_construction.py#L21-L139): original construction, scope admission and dependent receiving. Core result opening: `upload_journal_result_receiving.py` 13–61.
- [S14](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L8165-L8226): exact terminal source publication. Seal retries: lines 2086–2120. Cancellation: lines 3094–3155. Durable cleanup identity: `catalog_journal_validation_models.py` 20–56.
- [S15](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L2209-L2251): candidate rotation, retained capacity and retirement opportunities.

## Test references inspected only

- [T1](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/unit/test_upload_journal_validation_owner.py#L37-L415): outcomes 37–70; capacity 73–90; lost file 93–118; competing lease 121–150; crash/adopt 153–207; source deletion/stale CAS 211–260; manifest replacement 263–286; scheduler/independent journals 303–369; unopened replacement 372–415.
- [T2](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/unit/test_upload_journal_validation_custody.py#L42-L334): unconfirmed receiving 42–153; opening capacity 185–233; independent final readers 236–289; other-journal progress 292–334.
- [T3](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/unit/test_journal_validation_handoff.py#L94-L328): exact detached computation and source/terminal fences 94–198; independent catalog work 201–271; missing manifest link 287–300; process-exit setup 303–328.
- [T4](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/unit/test_journal_epoch_rank.py#L306-L351): rank marker corruption, including completed graph result. Related files: `test_journal_epoch_scan.py` 254–279; `test_journal_topology_capture.py` 285–312; `test_journal_epoch_union.py` 278–309; `test_upload_journal_source.py` 105–126.
- [T5](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/tests/test_validation_checkpoint.py#L183-L222): nine warm corruption/rejection variants added by d4d7.
