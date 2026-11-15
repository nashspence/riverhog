# Native phase reconciliation

## Scope and provenance

All **479 recorded retained-session advances reconcile exactly** with the source-derived dispatcher and child-phase algebra. This is an offline analysis of one instrumented run on clean source `3b47cbba770c31eb1487b50f7e3b16533c10e827`; it executes no product code, fixture, test, or profile replay.

Original input: `/workspace/scratch/21c38f9cc56d/native-phase-observation-r2/run.json`, 3,240,236 bytes, SHA-256 `019236fdc3b227d49753b54e79915b62d45cf44e788283d3843411611b33c00b`. Its archived probe has SHA-256 `65070c82c5904a3ec7e8ff7ce289f6eb11256f03576b919f1a4cdc114f76600a`. The report records complete status, intended exit 0, unchanged clean source, exact ordinary-oracle materialization/source matches, restored hooks, received original resources, and removed owned temporary workspace. Original process/lane exit receipts remain the outer controller's evidence.

The earlier `/workspace/scratch/21c38f9cc56d/native-phase-observation/run.json` records only the uv-output preflight assertion failure: zero Native advances and zero retained ordinary handles. It is excluded from every workload total here. The corrected run remained below its cooperative cap; its complete child interval, including building histories and ordinary validation oracles, was 12.992227 seconds. That interval is not the duration of only the instrumented advances and is not a throughput comparison.

## Inputs and Session calls

The initial frame contains six assertions: one activity, agent, artifact, description, occurrence, and state. The checkpoint adds no assertions. The correction replaces one agent assertion; the resulting effective projection still contains six assertions and one retired assertion. Each history was opened and validated independently. The shared first frame has the same exact bytes in all three histories.

| History | Source bytes | Frames | Scans S | Pending synchronizations F | Replay calls by frame E | Terminal calls | Session advances |
|---|---:|---:|---:|---:|---:|---:|---:|
| Initial | 4,913 | 1 | 1 | 1 | 103 | 2 | **107** |
| Initial + checkpoint | 5,903 | 2 | 2 | 2 | 103 + 16 | 2 | **125** |
| Initial + correction | 6,193 | 2 | 2 | 2 | 103 + 138 | 2 | **247** |

Every row satisfies `Session advances = S + F + sum(E) + 2`. Each successful advance publishes exactly one new parent version. No retry, capacity rejection, extra completed-state call, or worker replacement is present in this trace. These counts reconcile ordinary budgets: rows 64, source window 65,536 bytes, and work allowance 33,619,968 bytes.

Source owners: `journal_session.py` dispatch at 1513–1557; `_session_input.py` synchronization at 207–213 and admission at 250–251; `_entry_replay.py` dispatch at 2653–2673. All source paths in this note are under `/workspace/scratch/21c38f9cc56d/riverhog/packages/riverhog-provenance/src/riverhog_provenance`.

## Per-frame child algebra

For a normal frame, the source model is:

`E = 10 + P_entry + P_staging + P_finish + inactive_rows + 4*active_rows + P_generic + 2*typed_rows + P_row`.

The fixed ten cover frame admission; admission/completion of entry, staging and finish children; row exhaustion; recording; and publication. Each P is an actual prepare/apply invocation count, excluding that child's separate admission and later completion recognition.

| Frame | Active rows | Typed-row children | P_entry | P_staging | P_finish | P_generic | P_row | Correction addition | Reconciled E |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Initial, in each history | 6 | 4 | 3 | 3 | 25 | 18 | 12 | 0 | **103** |
| Checkpoint | 0 | 0 | 4 | 1 | 1 | 0 | 0 | 0 | **16** |
| Correction | 6 | 4 | 4 | 1 | 25 | 18 | 12 | 36 | **138** |

There are no inactive-row selections in these frames.

For each full six-assertion projection, the six generic children and four type-specific row children each perform exactly **one walk, one key cleanup, and one output cleanup** prepare. All ten walks finish in their first 64-step batch; none returns to `walk`. This does not measure the number of individual program steps inside that batch. Increasing the row quantum would not remove these separate cleanup phases in the observed cases.

SemanticFinish performs six row→lifecycle, six lifecycle→content, six content→externals, six externals→row transitions, then one row→done transition: **25 prepares**. Initial staging uses stage→observers, observers→stage, and stage→done, explaining its three prepares despite only six assertions. The later frames require two changed-directory cleanup calls: one removes the six earlier changed identities, then one observes exhaustion. Sources: `_generic_semantics.py:1554–1649,1738–1796`; `_semantic_finish.py:630–782`; `_assertion_staging.py:728–774,927–945`; `_entry_preparation.py:651–713`.

The correction addition is exactly `10 + 6 + 6 + 2 + 6 + 6 = 36`: fixed correction control; target prepares; aggregate-retirement prepares; seed prepares; and the two graph-retirement plan counts. Target work has one data batch and one phase transition in each of verify/apply/clean. Aggregate retirement takes six prepares across maps/findings/fixities/lifecycle_extrema. Seeding returns six active identities and inserts six changed-directory rows, then observes exhaustion. All six active assertions consequently undergo semantics and finish checks again.

**Correction to the original static wording:** graph R means **non-None returned retirement plans**, not nonempty deleted-row pages. Each graph returns six plans, one per retired relation, followed by one None exhaustion result. The 12 plans all report zero adjacency visits; there are no OnlineDag start or ordinary prepare invocations. Their indexed key/length fetches return no retired graph keys here; they still apply required relation/control transitions. Thus these histories exercise graph retirement control, not adjacency traversal or graph growth. Sources: `_entry_replay.py:2513–2553`; `_online_dag.py:49–56,1038–1123`.

## Starts, loads, and fixed control

| History | Session calls | Double-start/load calls | Single-start/load calls | `_start` calls | Parent loads within advances | Ledger + BEGIN execution attempts |
|---|---:|---:|---:|---:|---:|---:|
| Initial | 107 | 103 | 4 | 210 | 210 | 965 |
| Checkpoint | 125 | 119 | 6 | 244 | 244 | 1,193 |
| Correction | 247 | 241 | 6 | 488 | 488 | 2,779 |
| Total | **479** | **463** | **16** | **942** | **942** | **4,937** |

Each of the 463 replay admission/advance calls produces the expected second start/load. Every double call's captured phase/version/frame/row/child-ID fields agree; this is not an independently compared complete prefix snapshot. The second `_start` resets `_spent` to exactly the first reservation. Neither result implies that a future stale-state or certificate check can be removed.

The 4,937 count is a disjoint selection of Native admitted `execute` attempts owned by ValidationLedger plus BEGIN statements, out of 9,254 retained-advance `execute` attempts overall. It matches the fixed-control source model, including the optional predecessor-reference probe. It is not a count of physical page operations or Core catalog queries. Every advance has one fence and one parent publication. There are **482 explicit commit API calls**, not 479: terminal `JournalStore.freeze()` adds one call per history after the parent transition commit (`journal_store.py:1185–1186`). That extra API call does not establish an additional engine transaction or fsync.

The 942 `_start` calls account for 0.029826 seconds of exclusive observed thread CPU, including hook overhead, against 3.090955 seconds of retained-advance stage process CPU. They are visibly repeated but not the dominant observed CPU category. No removal-speedup estimate follows from those instrumented intervals.

## Source read reconciliation

| History | Framing bytes consumed | Window bytes returned | Window suffix read again | Direct exact JSON bytes | Leaf source bytes returned |
|---|---:|---:|---:|---:|---:|
| Initial | 4,913 | 4,913 | 0 | 4,911 | **9,824** |
| Checkpoint | 5,903 | 6,893 | 990 | 5,899 | **12,792** |
| Correction | 6,193 | 7,473 | 1,280 | 6,189 | **13,662** |

Each input is shorter than the ordinary window, so the first window returns the whole history but framing consumes only its first frame. The checkpoint/correction suffix is therefore returned again on the second scan. Direct atom reads total `source_bytes - 2*frame_count`, excluding each frame's separator/newline. Leaf bytes equal windows plus direct atom reads. The nested `read_exact` implementation of `read_window` is **not** added a second time.

Current-source checks total 109/127/249: one per advance, one at open, and one additional terminal check. All return true; these tiny OwnedSource checks do not represent Core worker/catalog fencing cost. The observed suffix rereads are bounded lookahead, not repeated whole-prefix validation.

## Proportionate next work

The trace supports separating repeated fixed phase/checkpoint work from useful semantic batches. It does not justify increasing concurrency, dropping fences, or changing phase ordering. Within this workload all walks already finish in one row-budget batch, and no graph adjacency work occurs. The correction's full six-row revalidation is an actual algorithmic multiplicity, not a source-byte-budget issue.

Do not prioritize removing `_start` solely because its count doubles. The stronger next investigation is the measured parent-checkpoint/control and transaction path: attribute canonical checkpoint encode/decode and explicit commit cost while preserving original source/worker/certificate checks. If that supports a change, prefer a small per-invocation metadata/serialization improvement before changing scheduling or checkpoint granularity. Any phase consolidation requires separate evidence for diagnostic order, source invalidation, capacity rejection, cancellation and durable restart; this successful-path diagnostic supplies none of those missing cases.

No timings or counts here are extrapolated to the historical publication, its 9,843 advances, 418 raised Core catalog attempts, or available parallelism. No growth-contract or whole-path qualification claim is added.

## Reproducible derived evidence

- [reconcile.py.txt](tools/reconcile.py.txt): SHA-256 `e2da5a148f5558d23b743bcd965004aa6f74372be7ab14ef9bc03f5771a746a2`.
- [reconciliation.json](evidence/reconciliation.json): SHA-256 `cf3951937fb6e7afded67dd6e93148b3691317bb8c467816e8ddf2dcedd59c20`.

The stdlib-only script pins the original input hash, verifies all archived input/oracle hashes, reconciles every phase and source byte term, distinguishes the failed preflight, and records its own hash. The JSON retains per-frame child transitions, graph returns, source receipts, budgets, source identity, and explicit scope limits. All final reconciliation guards pass.
