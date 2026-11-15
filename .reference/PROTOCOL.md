# Native Finish completion and parent continuation: protocol analysis

## Decision and scope

**The best narrow candidate is the final `SemanticFinish` batch followed by the parent-only `finish → recording` promotion. A direct fold is not safe under the current protocol: it removes a fresh completed-child validation and changes an externally observable durable checkpoint boundary.** The concrete counterexample is deletion or replacement of the completed `journal_semantic_finish` row between calls. The current parent remains in `finish`, so the next turn reloads the child and rejects. A parent already promoted to `recording` skips that child check. Source/worker/prefix/predecessor checks do not replace it.

This is a source/design reference on exact **d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e**, responding to [request 6890881026](https://github.com/nashspence/riverhog/issues/991#issuecomment-6890881026). It identifies one candidate and the exact missing owning contract. It proposes no implementation, loop, commit removal, checkpoint format, concurrency change or readiness cache. No product/test module was imported and no test, fixture, probe or product turn was executed. The companion `native-granularity-protocol.json` pins the source and supplied context.

The actionable next decision, if this edge is pursued, belongs to **Native terminal-child completion discharge into a parent continuation**, together with Core's mapping of fresh preparation, adopted progress and scheduling opportunities to that changed unit. A receipt or retained pointer would need defined validation and retirement semantics; merely retaining a `done` flag or a previously accepted digest would not supply that contract. A new schema or format is not asserted to be necessary.

## 1. Why this one edge is worth isolating

The received Native phase reference **8b639901c4dbe157dce904acec718a23f580c6ca**, on source **3b47cbba770c31eb1487b50f7e3b16533c10e827**, reports 107/125/247 retained advances for initial/checkpoint/correction journals and 25 `SemanticFinish.prepare` calls for six assertions in its initial packet. The current request supplies further instrumented and partial diagnostic reports. Those counts motivate inspecting boundaries; they do not establish that the counted operations or commits can be removed.

I compared the historical reference's product paths with d4d7. Exactly two product/test paths differ: `_session_checkpoint.py` and `test_validation_checkpoint.py`. The implementation delta returns the already decoded `PrefixState` from each fresh `ValidationLedger._prefix` call and reuses that local result in initialization/read; publication still performs its selected-prefix validation. It does not change `EntryReplay`, `SemanticFinish`, the parent CAS, transaction boundaries or Core readiness. The source receipt records both versions and the exact diff hash. Thus the historical phase graph is relevant to this candidate, without transplanting historical codec call counts into d4d7. [P01] [P21]

The received 482 commit API calls and their instrumented wall/thread-CPU intervals do not identify 482 independently removable durability costs. They include the actual owning admission/driver scopes. `JournalStore.freeze` itself calls `commit` before enabling query-only mode. No conclusion about fsync, lock waiting, storage contention, throughput, or the cost of the one edge follows from those aggregate intervals. This analysis does not reread or re-execute the new raw diagnostic artifacts supplied only by hash in the request. [P02]

The selected edge adds no second semantic assertion or scan to the proposed atomic unit: it concerns a terminal Finish result and the parent's continuation metadata. It is narrower than combining the 25 preparations, looping Native phases, or fusing recording, frame publication and terminal completion. Nevertheless, the current second turn performs live checks, so “parent-only” does not mean “effect-free” or “unnecessary.”

## 2. Exact current two-turn protocol

Let the starting parent have phase `finish`, version `v`, the selected child identity, and the digest of a nonterminal `FinishState`. The child has reached its `row` phase after completing its previous assertion. The next indexed change lookup returns no further assertion.

| Boundary | Current behavior | Durable/observable result |
| --- | --- | --- |
| A: prepare final Finish batch | `SemanticFinish.prepare` loads the exact child checkpoint/prefix and validates cursor links. In phase `row`, absence of the next changed assertion produces `after.phase = done`. The batch has no semantic writes on this branch and advances the child version. | Detached proposal only; no durable progress yet. |
| A: parent-owned transaction | `EntryReplay._child_step` calls the child's `_apply` inside `_commit`. Child CAS checks its input, attempt, exact old checkpoint and prefix. Parent fencing/publication checks the selected worker, parent, prefix and predecessor. | One transaction commits child `done` **and parent phase `finish`, version `v+1`, with child identity and digest**. |
| Return between A and B | Session returns actual progress. The Core owner ordinarily adopts it, releases the per-call slot lock, and the scheduler can perform its existing finalization/retirement/rotation behavior. | `finish` with a done child is an observable durable state, including after interruption. |
| B: next ordinary turn | Core repeats its complete preparation chain. Session/replay perform their fresh source/ledger/admission checks. `_child_step` reloads the exact Finish child and compares its digest with the parent. | Missing, replaced, corrupt or differently bound child state is rejected before promotion. |
| B: parent promotion | Because the child is done, `_next('finish', …)` yields `recording`; `_move` clears child identity/digest and commits parent version `v+2`. | Another durable parent state and another returned/adopted progress opportunity. |
| C: subsequent turn | In phase `recording`, the effective recorder/context check runs, then a transaction moves to `publish`. | Recording acceptance is separate from both A and B. |
| Later publication | Prefix acceptance, exact frame insertion, quarantine removal and parent advance happen in their existing transaction. Terminal validation/freezing/result receiving are later obligations. | Neither Finish completion nor `recording` is an accepted frame or a terminal result. |

The relevant code is `SemanticFinish.prepare` at 602–787, especially 630–644; `_apply` at 790–823; `EntryReplay._child_step` at 2464–2497; `_next` at 2441–2461; `_move` at 2308–2316; and dispatch at 2653–2673. Only GenericSemantics and EntryPreparation have an additional `child.result()` call in the done-child branch; Finish does not. Its child load, linkage checks and digest comparison still remain. [P03] [P04] [P05] [P06] [P22]

The child and parent already share transaction A. `_child_step` invokes the private `_apply`, not the standalone `SemanticFinish.commit`; there is no hidden child commit inside A that can simply be deleted. The candidate is eliminating or changing **B**, not removing duplicate nested transaction control from A. [P04] [P05]

## 3. Decisive counterexample to the direct fold

This is a deterministic source-derived fault interleaving, not an executed test or a claim that private checkpoints authenticate arbitrary caller-supplied databases.

1. Use a valid owned one-frame journal and reach the final Finish batch. In the baseline, return from A leaves parent phase `finish` with the exact done-child digest. In the hypothetical direct fold, the same final `_apply` instead publishes parent phase `recording`, clears the child fields as `_move` currently does, and returns immediately.
2. Between public calls, delete only the singleton row in `journal_semantic_finish` and commit that fault. Leave source bytes, Core preparation, worker/attempt, parent checkpoint, accepted prefix, predecessor frame and effective assertion data unchanged. Replacing the child's identity/binding or corrupting its checkpoint supplies related variants.
3. The current next turn dispatches through `_child_step`. `child.load` reaches `_read_checkpoint`; the exact header lookup fails and raises `StaleSemanticFinish`. With other variants, the algorithm/prefix/cursor checks or parent digest comparison reject. No parent promotion occurs. Warm child objects do not avoid this load; it reads the database freshly.
4. The direct-fold next turn sees parent phase `recording`. It checks the effective recorder/context, then moves to `publish`. Neither that branch nor the unchanged parent/source checks reads the deleted Finish row. It therefore lacks the baseline's rejection at this boundary.

This difference is enough to reject an “unchanged behavior” claim for the direct edit. It is not necessary to run to a full archive or invent a throughput benefit. The fault is in the owned continuation evidence that the existing protocol still requires before B; the proposed direct fold would discharge that requirement earlier without defining a replacement. [P03] [P05] [P06] [P07]

### Why the other fences do not recover this check

| Existing fence or later operation | What it actually verifies | Why the Finish-row fault remains distinct |
| --- | --- | --- |
| Native `Session.advance` source check | Exact current source binding and provider's currentness callback | The unchanged source does not describe the private Finish singleton. |
| Core `_OwnedSource.verify_current` | Worker identity/state, current staged source and graph-certificate manifest | These are catalog/source predicates, not the Native child checkpoint. |
| `ValidationLedger._read/_fence` | Parent algorithm/attempt/binding/worker, canonical parent, accepted prefix and predecessor | It does not load the live Finish child. Parent `recording` can be canonical with no child fields. |
| Parent `_publish` | One version increment, canonical target, exact selected prefix, old parent/worker/prefix CAS | It does not establish a discarded child's present state. |
| Effective recorder/context check | At most the indexed agent/context predicates in assertions | It does not inspect `journal_semantic_finish`. |
| Prefix publication | Prepared source/attempt binding, exact old accepted-prefix CAS, frame insertion and parent update | It does not restore the missing Finish validation. |
| Session terminal `_finish` | Complete input, exact accepted prefix, empty frame quarantine, current retained source commitments; then freeze and summary creation | It does not independently reload this child. |

These are positive ownership boundaries, not interchangeable redundant checks. The direct-fold rejection gap survives an ordinary warm continuation and is not repaired merely by reloading the parent on restart. [P07] [P08] [P09] [P10] [P11] [P21] [P22]

The current codec requires child identity/digest for `_CHILD_PHASES`; it does **not** forbid those fields in `recording`. Current `_move` clears them and current recording dispatch does not consume them. The missing piece is therefore a **meaning and validation rule for completion discharge**, not a demonstrated need for a schema change. Keeping fields without teaching an owning operation to validate/discharge them would not fix the gap. [P01] [P06]

## 4. Core C1: separate from the direct child counterexample

The [retained Core decision 6889902338](https://github.com/nashspence/riverhog/issues/991#issuecomment-6889902338) keeps one Native step and per-step catalog adoption. Its completed-rank counterexample remains valid: `rank._begin` checks that the exact last graph Result still exists with matching counters/state, while a warm `UploadJournalGraphCertificates.verify_current` checks Scan/Journal manifest fields and does not join that Result. The preparation functions also retain other dependent-row obligations. Fresh Native source/certificate callbacks are not a replacement Core readiness validator. [P12] [P13] [P14] [P25]

For a hidden Native loop that performs several old phases after checking Core preparation only once, deleting the last rank Result after the first committed phase can leave the next nongraph Native phase running without the old next Core check. Per-call Native source/worker/prefix checks cannot repair that omitted dependency observation. Resetting a Native work budget per internal iteration would create an additional aggregate-admission problem.

**That is not the same claim as saying C1 by itself proves invalid-output acceptance for this narrowly defined direct fold.** If a fold returns immediately after A and does not execute the recording check, another child prepare or frame publication, the next real Core call can still perform the full rank check before C. A rank deletion after the folded commit would then be detected on that next call. The direct completed-child deletion above is the decisive mismatch for this exact candidate.

Even for this narrow edge, Core must explicitly map what happens to old B: its fresh preparation observations, parent version, catalog adoption, stamp, scheduler rotation/retirement opportunity and `_schedule_finalization_if_ready` call. The phases would change from `finish/v+1` then `recording/v+2` to a proposed different atomic transition; this is not achieved by saying the remaining calls still do fresh checks. Separate point checks do not become one atomic all-dependency readiness snapshot. No hidden Native loop is authorized by this analysis. [P14] [P15]

## 5. Transaction, capacity, error and restart obligations

### Exact transaction and CAS

`EntryReplay._commit` starts `BEGIN IMMEDIATE`, fences the original parent snapshot before mutation, performs the child/private mutation, publishes the next parent, and commits; every `BaseException` takes its rollback path. Child `_apply` requires an existing owner transaction and does not commit or roll back it. `ValidationLedger._publish` requires exactly `old.version + 1` and conditionally updates the exact old checkpoint, worker and prefix. Prefix predecessor verification remains a live read. [P04] [P06] [P21]

Therefore a composition cannot call the existing two transitions under one transaction: B's detached load/planning assumptions and version/fence refer to A's published state. Nor can it simply suppress A's commit while calling a routine that requires a detached owner. A new atomic transition would have to define its one legal before/after pair and all validation needed before publishing it. Retaining two internal commits but one public call would instead create an unpublished intermediate prefix/progress problem. Neither route is an implementation detail.

### Aggregate admission

The existing public budget admits 1–64 rows, a work-octet allowance and a separate source-window allowance. `_start` resets `_spent` and reserves repeated fixed owner/source/checkpoint metadata; `_claim` and `_remaining` can refuse before admitting the next atom. Finish scans can stop with a partial page or raise before fetching its first unaffordable payload. These controls belong to the actual original unit. [P16] [P03] [P23] [P24]

The selected final row-to-done batch performs one absence probe and no semantic write payload on that branch. Folding its parent promotion would not justify a second semantic prepare or another row page. If research ever proceeds, its admission would still need to cover every live child/control check, canonical metadata width, digest, target checkpoint and transaction operation actually retained. The original row budget cannot be reused independently for two old phases. There is no general multi-phase allowance merely because the public API takes one budget object.

### Partial progress followed by refusal or error

In the current protocol A can commit, return and be catalog-adopted even if B later hits a capacity refusal, stale dependency, invalid child or interruption. That durable `finish` checkpoint remains a valid recovery boundary. A fused transaction would remove this particular intermediate state; a loop retaining A's commit but delaying adoption would keep the state without its old receiving opportunity. Both require a declared recovery/partial-result rule. [P05] [P15]

The existing layers distinguish errors. Session specially rethrows `JournalValidationCapacity`; other exceptions in its protected advance path retain failure state. The Core owner has its own capacity handling, stale-source handling, semantic-outcome publication, lost-workspace handling and original cleanup. A later exception cannot be recast as “no progress” merely because no grouped return was produced. A cold open can also adopt preexisting Native progress before a later refusal. The existing exact catalog source/job CAS must still prevent stale adoption. [P08] [P15] [P26]

### Interruption, restart and lost acknowledgement

Existing checkpoint tests inspect old-or-new atomic outcomes at transaction seams, stale worker/version/prefix rejection, and exact predecessor validation. Retained sessions replace workers and reconstruct the in-flight atom from owned storage; a public return is not needed for a durable Native commit to exist. A changed unit must identify the exact restart target for faults before child mutation, between child and parent mutations, before/after commit, before catalog adoption and after a lost reply. It must not reuse cached acceptance to bridge a missing child or predecessor. [P17] [P18]

For this edge, the required question is concrete: after final Finish application, **which exact persistent evidence is still live and must be revalidated before the parent proceeds, and which owner may retire it?** The current answer is the live child checkpoint/digest until B. No replacement completion-discharge contract is present in the inspected source. A newly named receipt would need to preserve the required rejection, restart and authority semantics; naming or hashing it is not itself a proof.

## 6. Terminal results and cleanup remain separate

Finish `done` means only that this semantic child has completed its work. It does not mean the frame is published, the journal is complete, the source is still terminally current, or storage can be released. The later recording check, exact accepted-prefix publication, terminal source/byte/hash/frame checks, freezing and borrowed summary creation remain separate owning operations. This reference does not propose fusing any of them. [P07] [P09] [P10]

The Core owner releases the actual slot only after original Native construction/storage receiving, including independent result readers. Session close releases its owned input/cache/storage path; retained files can remain for recovery, and borrowed readers retain storage until their own close. A commit, a complete child, a cancellation request, or a changed checkpoint version grants no disposal authority. Any future fault illustration must receive the original session/construction, SQLite resources, workspace lease and actual readers before deleting scratch. Existing tests establish those intended lifetime boundaries; they were only read here. [P19] [P18] [P27]

## 7. Exact missing owning decision and limited next work

The candidate needs all of the following **before** a code proposal could claim safety:

| Owning question | Required answer for this one edge |
| --- | --- |
| Native completed-child discharge | Whether a terminal Finish result may authorize parent continuation in the same atomic unit; what exact live evidence must still be verified; how deletion/replacement/corruption remains rejected at the required boundary. |
| Proof/retirement lifetime | Which owner retains and consumes that evidence across warm/cold restart, how exact child/parent/source/worker/prefix linkage is represented, and when the old child ceases to be a required live dependency. No representation is selected here. |
| Atomic progress/error contract | One explicit before/after transition, version increment, rollback/crash result, capacity semantics and lost-acknowledgement recovery; no hidden committed intermediate state omitted from the return/adoption protocol. |
| Core observation mapping | An explicit disposition for removed B's preparation/adoption/stamp/scheduler/finalization opportunity; full fresh preparation before every remaining real semantic turn. The accepted one-step decision cannot be bypassed by a Native loop. |
| Growth and evidence composition | Every actual child/control call remains accounted in its own domain, with an aggregate budget and metadata envelope. Existing single-child declarations cannot silently describe several old phases. Any conditional reduction is design arithmetic, not measured improvement. |

The most useful next work is a focused owning decision on this completion-discharge contract, using the explicit child-row fault above as an acceptance discriminator. The current tests already cover child prefix replacement, competing workers, corrupt child headers, parent worker/prefix/predecessor rejection, crash seams, source freshness and independent readers. They do not constitute validation of a hypothetical fused protocol. Do not replace the missing contract with a tiny successful fixture or a broad performance campaign. [P17] [P18] [P20]

Pending that decision, **retain both current transitions and commits**. This is a specific protocol blocker for the simplest proposed edit, not a finding that every Native commit is necessary forever. It does not reopen Core grouping, provide a new readiness validator, change the accepted observation policy, or establish any whole-path growth/throughput/G10/G12/G13 closure.

## Pinned source references

All references below are on d4d7. The JSON receipt includes exact Git blobs, SHA-256 values and line ranges, the historical two-file delta and the supplied request/context hashes.

[P01]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/_session_checkpoint.py#L86-L191
[P02]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/journal_store.py#L1185-L1188
[P03]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/_semantic_finish.py#L541-L787
[P04]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/_semantic_finish.py#L789-L846
[P05]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/_entry_replay.py#L2441-L2497
[P06]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/_entry_replay.py#L2294-L2316
[P07]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/journal_store.py#L1293-L1312
[P08]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/journal_session.py#L1513-L1573
[P09]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/_journal_prefix.py#L712-L797
[P10]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/journal_session.py#L1456-L1510
[P11]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_validation_owner.py#L402-L438
[P12]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_rank.py#L245-L291
[P13]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_certificates.py#L660-L751
[P14]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L2122-L2251
[P15]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_validation_owner.py#L1955-L2122
[P16]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/_entry_replay.py#L2230-L2252
[P17]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/tests/test_validation_checkpoint.py#L141-L315
[P18]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/tests/test_retained_journal_session.py#L77-L403
[P19]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_validation_owner.py#L1875-L1893
[P20]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/tests/test_semantic_finish.py#L321-L593
[P21]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/_session_checkpoint.py#L393-L566
[P22]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/_entry_replay.py#L2555-L2673
[P23]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/journal_session.py#L66-L89
[P24]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/_entry_replay.py#L75-L84
[P25]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/unit/test_journal_epoch_rank.py#L306-L351
[P26]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_validation_jobs.py#L463-L544
[P27]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/journal_session.py#L1575-L1606
