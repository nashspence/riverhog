# Native phase and invocation analysis

Static inspection only at `3b47cbba770c31eb1487b50f7e3b16533c10e827`, branch `reference/external/991-native-phase-cost`. No product, fixture, test, diagnostic or benchmark was executed for this analysis. The working tree was clean when inspected. Paths below are relative to `/workspace/scratch/21c38f9cc56d/riverhog`; line numbers refer to that exact source.

## Main finding

The ordinary 64-row budget is a bound on one selected child phase, not a promise to finish 64 assertions per Session call. `JournalValidationSession.advance` dispatches one durable source/entry action, and `EntryReplay.advance` dispatches one row, lifecycle, semantic child, graph, recorder or publication action. Each child has a separate admission and completion-recognition transition. Even small inputs therefore pay many fixed checkpoint rounds. Some sequential dependencies are required by validation/error order; the choice to expose every small phase as a distinct scheduling/checkpoint unit is a separate implementation granularity question. Static inspection establishes that distinction, not the fraction of measured elapsed time attributable to it.

Relevant owner contracts explicitly distinguish per-unit conformance from complete-stream growth. In particular `_online_dag.py:198–208` does not certify an amortized whole-stream bound, and `journal_session.py:675–698` excludes provider implementation cost and physical lock/fsync/allocation cost from its logical counters. The retained 9,843-advance publication observation must not be treated as a whole-path growth proof.

## Work budgets and exact dispatch

- `journal_session.py:66–75`: default `rows=64`, `source_octets=65536`, work `octets=4*PROVENANCE_JOURNAL_ENTRY_BYTES_MAX+65536`. The entry maximum is 8 MiB (`constants.py:14`), so the work byte allowance is 32 MiB plus 64 KiB. It is separate from the F/C/K/Q fixture geometry and the source read window.
- `_entry_replay.py:75–84`: ReplayBudget accepts rows 1–64; a positive work-byte budget can still produce an explicit capacity rejection.
- `journal_session.py:1513–1562`: source fence, parent load and `_start` precede dispatch. Terminal/done handling, entry advancement, input synchronization, frame admission, terminal movement and source scanning are distinct returns.
- `_entry_replay.py:2653–2673`: one parent load and start; warm/cold current-entry check; then exactly one graph, row-admission, lifecycle, recording, publication or child branch. It does not loop over remaining phases merely because row budget remains.
- `_entry_replay.py:2230–2252`: `_start` reserves a fixed number of metadata copies using their actual byte widths; `_claim` and `_remaining` reject insufficient capacity. “Fixed” here means a fixed-shaped envelope, not zero-cost metadata or fixed scalar lengths.

## Successful-path invocation algebra

These are source-derived equalities under an uninterrupted successful execution, with no capacity retries, reopens or extra post-completion calls. They are not measurements of the retained publication fixture.

Let `F` be frame count; `S` be retained source scan calls; and `E_j` be EntryReplay admit/advance calls from admission through publication of frame j. Then:

`Session advances = S + F + sum(E_j) + 2`.

The `F` term is pending-input synchronization after each published frame: `RetainedInput.admitted()` does nothing; a subsequent frame-phase call publishes `pending=None` (`_session_input.py:207–213,250–251`). The two terminal calls are frame→terminal and terminal→done (`journal_session.py:1531–1551`). Completed-session polling adds further calls; failed/retried/restarted work requires separate terms. Stream mode does not have the retained synchronization term and separately counts empty/EOF iterator pulls (`journal_session.py:236–269,283–285`).

For one normally completed child c, invocation count is `2 + P_c`: one child admission, `P_c` prepare/apply calls, then a later load/digest/done check and parent move. This is literal `_child_step` control flow (`_entry_replay.py:2464–2497`), including calls to `result()` for GenericSemantics and EntryPreparation.

For a non-correction frame, let A be active changed assertions, I inactive changed assertions, and T the subset whose type needs the additional row program. Define `P_entry`, `P_staging`, `P_finish`, `P_generic(r)` and `P_row(r)` as the actual successful prepare/apply counts for those child invocations. Then:

`E_j = 10 + P_entry + P_staging + P_finish + I + sum_active(4 + P_generic(r) + 1[r in T]*(2 + P_row(r)))`.

The fixed ten are frame admission; admission/completion of entry, staging and finish children; changed-row exhaustion; recording; and publication. Each active row additionally pays row selection, generic admission/completion, lifecycle and its generic execution; row-specific semantics add a separate child. Sources: `_entry_replay.py:2327–2355,2441–2497,2593–2645,2653–2673`.

Useful decompositions:

- EntryPreparation has journal identity, entry identity and changed-directory reclamation phases. With M previous changed-directory rows and a fixed sufficient effective row quantum q, its prepares number `3 + ceil(M/q)`: two identity phases, deletion batches and a final empty deletion. Actual byte admission may reduce q. It removes changed-identity metadata, not assertion payloads (`_entry_preparation.py:634–713`; `journal_store.py:1132–1144`).
- GenericSemantics does up to `rows` program steps for one assertion in a walk batch, then separate key and output reclamation phases. A successfully completed invocation has at least walk, keys and outputs prepares; 64 steps is not 64 assertions. Byte capacity, graph requests and cleanup cardinality add batches (`_generic_semantics.py:1554–1649,1738–1796`).
- AssertionStaging may stage up to `rows` assertions, but an activity with associations breaks to separate observer-membership batches; byte admission can end a batch sooner (`_assertion_staging.py:728–774,778–790,824–858,927–945`).
- SemanticFinish selects one changed assertion in its row phase. An active assertion passes lifecycle, content and externals phases, even when there are no external references. Thus its prepare count is at least `4*A + I + 1`, with additional diagnostic/reference scan batches. Indexed scans advance physical-row cursors under the row/byte budget and preserve error order (`_semantic_finish.py:630–782`; declared rationale at :337–356).

A correction adds target verification/application/reclamation, aggregate retirement, two graph replacements/retirements, and reseeding. If `P_targets`, `P_retire`, `P_seed` are child prepares and `R_derivation`, `R_specialization` are non-None returned retirement-plan applications, the additional outer contribution is `10 + P_targets + P_retire + P_seed + R_derivation + R_specialization`: three admission/completion pairs, two replacements, two graph exhaustion transitions. A returned plan can carry relation/control progress without deleting any rows; it does not imply a nonempty graph. The active-row/finish terms now cover the reseeded effective projection. Sources: `_entry_replay.py:2441–2460,2513–2553`; `_correction_projection.py:467–529`. The later tiny diagnostic observes six such plans per graph and a separate None exhaustion return; see [the measured reconciliation](RECONCILIATION.md).

This algebra permits a small diagnostic to reconcile every top-level advance with its causal phase rather than infer costs from the nominal source payload size.

## What is sequential, and what is scheduling granularity?

Required order within one owned journal:

1. Exact frame/source/schema/chain checks precede semantic work. The immediately previous accepted reference and continuation hash bind each frame (`_journal_prefix.py:646–705`).
2. Child admission precedes its continuation; detached preparation precedes fenced application; a completed child is checked before the parent moves (`_entry_replay.py:2464–2497`).
3. Staging precedes generic and type-specific checks; lifecycle and post-row checks precede recorder/context checks and publication (`_entry_replay.py:2441–2460,2593–2673`). These stages share the same mutable effective projection and preserve the first native failure.
4. Entry publication atomically updates prefix/frame storage and parent checkpoint; only then may the next frame be accepted (`_entry_replay.py:2564–2587`).

The single admitted frame, singleton child checkpoints, shared SQLite connection and exact parent CAS do not expose independent work inside one current journal merely because two workers are available. Parallel execution of successive phases/rows would require a different ownership/error-order design; it is not justified by low utilization alone. Independent sessions/sources may be schedulable under Core's separate admission and concurrency policy. The many fixed transitions could potentially be processed under a bounded owner loop while retaining each commit, cancellation point and fence, but that would be a different runtime change with different growth and interruption obligations; this analysis recommends measurement before designing it.

## Exact `_start` candidate

`JournalValidationSession.advance` invokes `_replay._start` after its own load at `journal_session.py:1527–1529`.

- An active non-frame branch then calls `EntryReplay.advance`, which loads and calls `_start` again (`journal_session.py:1537–1540`; `_entry_replay.py:2653–2655`).
- A pending-frame branch similarly calls `EntryReplay.admit`, which loads and calls `_start` again (`journal_session.py:1545–1548`; `_entry_replay.py:2330–2333`).
- Source scan/synchronization, terminal and already-done branches do not take that inner call.

`_start` checks exact certificate-binding equality, stores the budget, encodes the checkpoint to size its fixed metadata reservation, resets `_spent`, and checks remaining capacity. It issues no database statement itself. The second reservation replaces the first; it does not double the effective capacity charge. For successful calls, the syntactic count is one outer start per Session advance plus one inner start per actual replay admit/advance.

This establishes repeated invocation, not measured redundant cost. In particular the two parent loads are separate from `_start`, and the second load can observe changed worker/parent state. Standalone EntryReplay methods own their own checks and their contracts declare their load. Removing or moving a start changes when certificate/current metadata and capacity are checked. A safe follow-up should first count and time start, load and publication separately, including both frame-admission and non-frame paths; any later optimization must retain all ownership checks, standalone behavior, resource admission and failure ordering.

## Control-query algebra and physical costs

Let h be 1 when the accepted prefix is nonempty, else 0. On a successful unchanged-prefix path:

- `ValidationLedger.load` / `_fence` each execute `2+h` SELECTs: parent, prefix, optional exact predecessor reference (`_session_checkpoint.py:416–447,485–525`).
- `_publish` executes `2+h` statements: prefix plus optional predecessor, then conditional parent UPDATE; it validates internally constructed checkpoint JSON and exact one-version advancement (`_session_checkpoint.py:528–568`).
- A warm active Session branch using an ordinary `_commit`/`_move` consequently has `9+4*h` parent-control `db.execute` calls: two detached loads, BEGIN IMMEDIATE, transactional fence, publication. Child/semantic SQL, source-provider calls and the DB commit operation are additional. This is a static count of explicit calls, not a physical I/O or latency measurement.
- Progress construction itself does no SQLite or source I/O; it decodes fixed metadata, hashes exact length-delimited checkpoint pieces and finalizes the accepted SHA continuation (`journal_session.py:129–177,190–217`). Calling public `progress()` additionally performs source verification and a parent load (:1565–1573).

Fixed statement count does not imply constant physical time. The checkpoint owner declares indexed VM work `16384 + 2048*ceil(log2(2+retained_rows))`, actual metadata widths, and explicitly excludes lock waits, fsync, I/O and B-tree byte comparisons (`_session_checkpoint.py:222–308,358–388`). Native SQLite work is not the same counter domain as the historical Core catalog execution attempts. Those 418 raised attempts require exception/site/SQL classification; no Native-only probe can retrospectively label them errors or waste.

For attribution, use disjoint leaf categories: source-provider verification/read bodies; native ledger/control SQL and serialization; semantic/graph SQL and computation; transaction/lock/commit wait; pure progress encoding; observer handling outside primitive accounting. Session, EntryReplay and child inclusive times nest and must not be summed.

## Whole-input and repeated work

Ordinary continuation is designed not to reread accepted frame payloads. `_entry` returns a checked warm current-frame cache, or restores only the quarantined current frame after checking its length and digest (`_entry_replay.py:2270–2292`); `_row` similarly restores only the selected assertion (:2361–2382). Prefix hashing resumes SHA state and adds the current encoded frame (`_journal_prefix.py:695–705`). Maintained tests deny historical `frames.value` reads while continuing and compare frame-hash invocation counts at row budgets 1/64 (`test_entry_replay.py:274–316`). These are existing tests inspected, not tests rerun here.

Retained source scanning has bounded lookahead: `_session_input.py:221–228` requests up to the source window, while framing stops after one frame (`framing.py:148–168`). A trailing part of a returned window can therefore be fetched again next time; the cursor only advances over consumed bytes. With ordinary full windows W and sufficient work bytes, scan calls are `sum_j ceil(frame_encoded_octets_j/W)`; shorter valid provider windows can increase this. Total returned window bytes can exceed source bytes by bounded per-frame suffixes (at most about `F*(W-1)` extra), and each framed JSON atom is then read exactly once for admission (:234–248). This is a source-read multiplicity candidate for counting, not a repeated whole-prefix hash or an observed bottleneck. Preserve the one-frame frontier: a later framing failure must not outrank an earlier semantic failure.

Corrections do perform work over an existing effective projection. `CorrectionWork.retire` clears the four aggregate relations (`maps`, `findings`, `fixities`, `lifecycle_extrema`) in bounded pages; seed enumerates every active assertion (`_correction_projection.py:38–45,467–529`). Old graph runs are retired by six relation kinds in bounded key/byte plans (`_online_dag.py:49–56,1038–1123`). Then active assertions are semantically checked again. Therefore total work across corrections includes the sum of effective projection sizes and graph/semantic work at each correction; a per-page bound alone cannot prove linearity in newly appended bytes. The current algorithm uses this to reestablish validation after removals/replacements. Whether less invalidation is safe requires an owner-level dependency proof, not a concurrency increase or caching old acceptance.

For uncertified graph edges, level/fuel repair includes initialize, queue-pop, backward/forward adjacency pages, promotion, insertion, undo/reclamation phases (`_online_dag.py:719–787,877–951`). Actual graph visits, cleanup and undo must be counted independently of phase count. Certificates can replace only graph traversal, after exact source/frame/edge/rank verification; native reference/semantic checks continue (`_generic_semantics.py:1060–1096`). The owner explicitly leaves whole-stream amortization outside this unit contract (`_online_dag.py:198–208`).

## Fences, recovery and existing obligations

- Source verification is current source/job authority and is contractually not a source-content scan (`journal_session.py:45–53`). Retained open binds source, catalog and optional certificate manifest, requires current source, and transfers cleanup custody only on success (:1307–1323,1329–1351,1435–1445).
- Every advance/progress checks current source. Terminal checks exact journal identity, total bytes, SHA, frame count, no unfinished frame and matching accepted source (:1469–1497,1522–1526,1568–1573).
- Reopen requires a strictly newer worker epoch; replacement fences the previous checkpoint before changing epoch (`journal_session.py:1391–1426`; `_session_checkpoint.py:574–595`). Child changes and parent publication are committed atomically; failed transactions roll back (`_entry_replay.py:2294–2306`).
- Capacity exceptions remain retryable without poisoning the session; other failures are retained (`journal_session.py:1558–1562`). Independent result reader lifetime and close custody remain distinct from successful validation (:1498–1509,1575–1605).

Relevant existing tests inspected: `test_retained_journal_session.py:76–137` (bounded resume and completed reopen without source reread), :141–202 (foreign binding, terminal fence and final commitment), :364–420 (every-unit source check and pure progress); `test_entry_replay.py:98–152,274–327` (budgets/cold resume/correction/history/capacity); `test_journal_graph_certificates.py:124–148,237–264,299–333` (only graph traversal replaced, manifest change rejected, committed certificate not repeated). Growth fixtures preserve separate uninstrumented native oracles, exact table state, child call counts and accepted prefix (`test_entry_replay_advance_growth.py:563–626`); Session/ledger phase witnesses remain separate (`test_journal_session_growth.py:1090–1170`; `test_validation_checkpoint_growth.py:202–259`). No claim is made here that these tests were executed on this branch.

## Narrow next work

A finite tiny Native-only diagnostic can reconcile the algebra and determine which repeated controls matter: ordinary defaults; one minimal initial journal, an appended no-change checkpoint, and only if time permits a small correction or a single graph edge. Record input bytes/frames/assertion kinds, every Session/EntryReplay/child phase count, actual row/step/graph work, `_start` and ledger invocation counts, source requested/returned/consumed bytes, successful/raised SQL classified by owner and exception, and disjoint control/semantic/progress timing. Preserve exact result/hash/materialization checks and original cleanup/exit. Keep the explicit approximately two-minute cap and report incomplete diagnostics as incomplete. Do not infer product throughput from these inputs or change concurrency, fences, phase ordering or resource bounds before the result justifies a separate source-owned proposal.
