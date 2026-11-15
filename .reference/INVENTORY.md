# Native transition granularity: historical inventory and growth boundary

## Finding and scope

**Keep the runtime unchanged. The one narrow candidate worth an owning design decision is the parent-only `finish` → `recording` promotion after `SemanticFinish` has already committed `done`.** The historical trace contains five instances of this edge. It does not justify merging the 25 Finish preparations for six assertions, looping inside Native, changing transaction policy, or skipping the next Core readiness check.

The decisive gap is a **Native completion-discharge contract**. Currently the intermediate parent checkpoint retains the completed child's identity and digest. The next public call reloads that child and checks its digest before advancing the parent. Combining the final child application with parent promotion would remove that later observation. A deleted or replaced child checkpoint between those calls is rejected by the current path; the proposed next `recording` path does not reload Finish. Deciding that a successfully completed child may cease to be authoritative at the earlier commit requires an explicit owning rule, including restart and corruption behavior. It is not a conclusion established by the five successful historical occurrences. [S1, S2, S3]

This note uses only checked-out source and existing artifacts. No product module was imported, fixture constructed, database opened, validation method invoked, test run, or profile repeated. The JSON companion is an offline inventory, not a new measurement or qualification result. The original lossless raw remains at the exact published reference; the companion copies the ten selected pair records without normalizing their identities or checkpoint hashes. [E1]

## Exact source and evidence relationship

Current source is `d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e`, tree `98cf298e60e4ba9c178229eae907e164265f6e48`. The historical Native phase reference is `8b639901c4dbe157dce904acec718a23f580c6ca`, whose sole parent is `3b47cbba770c31eb1487b50f7e3b16533c10e827`. Decompressing that reference's `.reference/evidence/observation/run.json.gz` yields **3,240,236 bytes**, SHA-256 **`019236fdc3b227d49753b54e79915b62d45cf44e788283d3843411611b33c00b`**, exactly equal to the retained local `native-phase-observation-r2/run.json`. The compressed blob is 137,314 bytes, SHA-256 `bf71e8e672438e7f6026d98ba8be06d03ad598597945e066c814c4c9fc273f0b`. [E1]

The complete `3b47cbba` → `d4d7` tracked diff contains exactly two files: `_session_checkpoint.py` and `test_validation_checkpoint.py`. The private prefix reader now returns its fresh raw row together with the parsed `PrefixState`; initialization and `_read` reuse that same-invocation state. `_fence` still performs its fresh `_read`, and publication still independently parses the selected prefix. SQL reads, current prefix/predecessor checks, worker and binding predicates, canonicality checks, CAS and transactions remain in source. The nine new warm-corruption cases are source evidence here; they were not rerun. Finish, EntryReplay, Session, correction, semantic, Core and growth-declaration code inspected for this note are byte-identical across those two product revisions. [S3]

The receiving comment reports a separate matched codec diagnostic with prefix decodes **5,185 → 3,758**, all **1,903** ValidationCheckpoint parses retained, and unchanged 479 progress/control traces, other method counts, DBAPI attempts, source deliveries and results. Those codec figures are **receiving reports**, not counters present in this older 71-hook raw. Do not transfer the older raw's elapsed times to `d4d7`. [E2]

## Historical work, with no inferred removals

Each history was independently opened and validated. The shared initial frame has six assertions: an activity, agent, artifact, observation, occurrence and state. The checkpoint adds no assertions. The correction replaces one agent assertion and leaves six effective assertions, with one retired assertion. All source and ordinary materialization hashes are retained and matched to the published bytes. [E1]

| Independently validated history | Literal bytes | Frames | Source scans | Pending synchronizations | Entry replay calls per frame | Terminal calls | Session advances |
|---|---:|---:|---:|---:|---:|---:|---:|
| Initial | 4,913 | 1 | 1 | 1 | 103 | 2 | **107** |
| Initial + checkpoint | 5,903 | 2 | 2 | 2 | 103 + 16 | 2 | **125** |
| Initial + correction | 6,193 | 2 | 2 | 2 | 103 + 138 | 2 | **247** |

The accounting identity is `advances = scans + synchronizations + entry replay calls + terminal calls`. It describes these actual calls, not a cost formula used to fabricate observations. Each successful call in this trace publishes one parent version. No rejection, retry, replacement worker or extra already-done call appears in these 479 records. The public progress phase `source` renders the durable parent phase `frame`; the JSON index keeps those two fields separate. [E1; S4]

The child conductor partition is also complete:

| History | Child admissions | Child prepare/application pairs | Child completion recognitions | Total `_child_step` calls | Finish prepares | Finish loads |
|---|---:|---:|---:|---:|---:|---:|
| Initial | 13 | 61 | 13 | 87 | 25 | 26 |
| Initial + checkpoint | 16 | 67 | 16 | 99 | 26 | 28 |
| Initial + correction | 29 | 135 | 29 | 193 | 50 | 52 |
| **Total** | **58** | **263** | **58** | **379** | **101** | **106** |

These are distinct phases. Admission calls the selected `prepare_admission`/`_admit` pair. An unfinished child is loaded and digest-checked, then executes one `prepare`/`_apply` pair. A done child is loaded and digest-checked, executes its required `result` only for EntryPreparation or GenericSemantics, and commits the parent's next phase. Finish has no `result()` call in that branch. Those source facts explain the partition but do not make every completion recognition optional. [S1]

### What the 25 Finish preparations actually do

For each full six-assertion effective projection, the original child records contain:

| Actual Finish transition | Prepares per full six-assertion projection | Durable behavior today |
|---|---:|---|
| `row` → `lifecycle` | 6 | Select one next changed active assertion; publish its exact current identity. |
| `lifecycle` → `content` | 6 | Evaluate the applicable lifecycle branch; publish its outcome. These inputs never enter `invalidation` or `usage` scans. |
| `content` → `externals` | 6 | Run applicable address/fixity work and prepare any writes before reference checks. The observation assertion has real content. |
| `externals` → `row` | 6 | Observe the end of the indexed reference scan and complete that assertion. |
| `row` → `done` | 1 | Observe changed-row exhaustion and publish child completion. |
| **Total** | **25** | **25 separate child prepare/application and parent commit/return boundaries.** |

The initial projection is processed once in each history, and the correction processes its effective six assertions again: four full projections. The empty checkpoint projection has one additional `row` → `done` prepare. Thus `4 × 25 + 1 = 101` actual Finish preparations. There are five Finish admissions and five later parent completion recognitions. The proposed narrow edge leaves **all 101 prepares and their semantic obligations intact**. [E1; S2]

Within one Finish `prepare`, the owner already performs multiple bounded operations: a lifecycle branch may read both timestamp bounds; a content branch can check address completeness, fetch its admitted atom, compute fingerprint/fixity facts and stage bounded writes; a reference phase loops over at most the admitted row quantum. Those internal actions share one returned proposal. The `elif` dispatch does **not** continue into the next Finish phase after selecting a transition. A phase change itself is returned, then independently applied. Its child checkpoint CAS, writes, parent fence and parent checkpoint publication share one transaction. [S1, S2]

Generic and type-specific row semantics are similarly distinguishable. Each full projection has six generic plus four type-specific children; all ten complete `walk` in one preparation, then have one `keys` cleanup and one `outputs` cleanup preparation. The trace records those child methods through GenericSemantics. It does not count every internal program step, row-specific handler or VM visit. Raising the row quantum cannot eliminate the separately dispatched cleanup phases shown by these inputs. [E1; S5]

### Correction remains a separate growth obligation

For the additional correction frame, the observed 138 EntryReplay calls comprise the ordinary six-row work plus the correction target, retirement, graph and reseeding path. The correction-specific addition is **36** calls: ten fixed control/admission/completion transitions, six target prepares, six aggregate-retirement prepares, two seeding prepares, and six non-None retirement plans for each of the two graph kinds. The two graph exhaustion calls belong to the fixed control accounting. All twelve non-None plans report zero adjacency visits; two further retirement returns are `None`. No ordinary OnlineDag start/prepare appears. [E1]

The seed owner selects **all active effective assertions** into the changed directory; the trace then performs twelve generic plus eight row admissions and 60 GenericSemantics preparations over the two correction-history frames, followed by 50 Finish preparations. Reducing one bookkeeping edge per frame would not change that repeated effective-projection work. In general, total work must retain the sum over the effective projections actually revalidated, with their semantic/program/adjacency work and restart work. These tiny histories establish neither a correction-wide linear bound nor nontrivial DAG growth closure. [S6]

## The five exact candidate occurrences

The raw uses a global zero-based `ordinal`; a history's parent version restarts at zero. The following pairs are exact consecutive records in the `retained_advances` stage. On the first call, Finish `prepare.after_state` and `_apply.state` both say `done`, while returned parent progress remains `finish`. The second call has only `SemanticFinish.load` in its child return records, returning that same done/version, before public parent progress becomes `recording`. The child ID is identical across each pair. [E1]

| History / frame | Global final-Finish / promotion ordinals | Parent versions across the pair | Finish final version | Promotion's admitted `execute` / `fetchone` / explicit commit calls |
|---|---:|---|---:|---|
| Initial / 0 | 100 / 101 | 100 → 101 → 102 | 25 | 12 / 10 / 1 |
| Checkpoint / 0 | 207 / 208 | 100 → 101 → 102 | 25 | 12 / 10 / 1 |
| Checkpoint / 1 | 225 / 226 | 118 → 119 → 120 | 27 | 15 / 13 / 1 |
| Correction / 0 | 332 / 333 | 100 → 101 → 102 | 25 | 12 / 10 / 1 |
| Correction / 1 | 472 / 473 | 240 → 241 → 242 | 51 | 16 / 14 / 1 |

Across those **five historical promotion calls**, the trace records five source-currentness port calls, ten parent ledger loads, ten `_start` calls, five child loads, five parent fences, five parent publications, **67 admitted `execute` attempts, 57 `fetchone` attempts and five explicit commit API calls**. These describe the existing edge. They are **not** a prediction that all of this work can be removed: a completion proof or receiving rule may need to retain or relocate some checks. No per-edge timing or speedup is inferred from the aggregate phase scopes. [E1]

For context, the complete historical retained-advance stage has **9,254** admitted `execute` attempts and **482** explicit commit API calls. The latter comprise 479 parent transition commits plus one terminal `JournalStore.freeze()` commit call per history. Across setup, retained advances and terminal materialization, the raw records **9,524** execute attempts, 12 executescript attempts and **497** explicit commit calls, including 15 setup commits. Commit API calls are not a count of independent engine transactions or fsyncs. The five candidate occurrences are a historical opportunity count, not five guaranteed saved commits or a `5/482` performance estimate. [E1; S4]

## Current growth contract and a hypothetical one-edge envelope

Ordinary Session admission is rows **64**, work octets **33,619,968**, source octets **65,536**. `ReplayBudget.rows` permits 1–64; the work-octet budget is positive. `_start` reserves actual fixed control bytes using `16384 + 16 × (binding width + prefix widths + canonical parent-checkpoint width)`. Cold current-frame/row restoration subtracts its admitted bytes before the child receives `_remaining()`. Repeating `_start` would reset rather than accumulate that reservation. These are logical work/byte admissions, not total process-memory or disk-allocation bounds. [S1, S4]

Finish's existing declared unit has `retained_rows = N`, `row_budget = R`, `byte_budget = B`. Its source bounds include `sql_statements ≤ 64 + 4R`, `returned_rows ≤ 32 + 3R`, `payload_fetched_octets ≤ B`, `metadata_octets ≤ 65,536 + 512R`, and `written_value_octets ≤ 4,096 + B`; up to three rows can change in the standalone/application scope. Detached planning changes no rows or written-value bytes; application fetches no payload and neither commits nor rolls back its parent's transaction. These are declared envelopes, not observations from the tiny histories. [S2]

The selected hypothetical edge can remain smaller than a multi-phase semantic unit:

| Dimension / obligation | Required candidate limit or behavior, if separately authorized |
|---|---|
| Selected child | One already admitted Finish child with its exact current identity, source, prefix and parent digest. |
| Semantic work | At most **one** unchanged Finish `prepare` and **one** `_apply`; no second phase prepare, next row, recording work or frame publication. |
| Administrative work | At most **one** terminal parent promotion when that applied proposal returns `done`; then immediate public return. |
| Row admission | The same single plan's `R`, at most 64; no fresh row allowance for a second phase. |
| Octet admission | The same current call's residual `B`; no second `_start` or budget reset. Any new completion-proof bytes, metadata or callbacks must be admitted explicitly. |
| Source input | No new window, exact atom or iterator pull in this narrow branch. Existing source/currentness checks stay owned and observed. |
| Transaction | One existing child application plus one parent fence/publication/commit. No independent child commit, second parent version, accepted-prefix publication or terminal result. |
| Partial work | Capacity/error before the owner commit leaves the preceding exact parent/child checkpoint. A committed outcome remains authoritative across reply loss/reopen; cleanup receives originals. |
| Child evidence | Record the actual plan/application, prefix/fence/publication, payload and metadata counters. A fused public root must not erase the child observation. |

This is a **proposed admission shape**, not an implemented API or an approved bound. The current `_child_step` declaration explicitly partitions admission, unfinished progress pair and completed-child recognition. EntryReplay.advance declares at most one selected row/lifecycle/child/graph/recording/publication branch. Session composes actual child calls and source-port observations. A new conditional terminal-promotion rule must update the owning transition/declaration/observer expectations; reducing public call count alone cannot establish compliance. Separate child bounds retain their own table domains and exact input widths. Parent inclusive counts already include descendants: do not add overlapping child totals a second time. Provider callbacks remain charged to their owning scopes. [S1, S4]

For any broader phase combination, the missing aggregate allowance is stricter: independently cap the number of attempted phases, sum row/step visits and admitted octets across all executed phases, retain each plan/application and each failure, and account for control bytes before admission. Reusing a 64-row allowance and a full byte budget for every iteration is not one bounded existing unit. This note does not propose such a loop or derive a new total bound for it.

## Why the one edge still needs a protocol decision

### Native child completion and corruption

Current order is: prepare the final Finish batch outside a transaction; apply its exact CAS inside the fenced parent transaction; publish parent `finish` with the done child's digest; commit and return. On the next public call, Session checks source/currentness, the ledger loads/fences current parent/prefix/predecessor authority, and `_child_step` reloads the child's exact checkpoint and compares its digest before `_move(recording)`. [S1–S4]

A static counterexample is deletion or replacement of `journal_semantic_finish` after the done commit while the parent, accepted prefix and source remain unchanged. Current `SemanticFinish.load` rejects a missing/superseded record or the parent digest comparison rejects changed content. A combined terminal parent promotion would already have cleared the child fields; the next `recording` branch performs the recorder check rather than a Finish load. The two protocols have different corruption/rejection observations. This is not a claim that an invalid archive would necessarily be accepted: it identifies the exact authority that would be discharged earlier. A completion receipt, atomic retirement rule, or explicit permission to stop consulting the completed checkpoint must be owned and specified before changing the boundary. [S1, S2, S3]

Existing tests establish the importance of these individual boundaries: stale prefix proposals are fenced; finished workers still check their exact inputs; late scan failure preserves the prior committed checkpoint; a byte-limited scan can commit preceding admitted rows and then reject the next atom without progress; real process-death tests cover prepared/header/before-commit/after-commit seams. They are source evidence for existing behavior, not qualification of a new fused transition. [S7]

### Core readiness and observation

Core's current service unit freshly reads source/capture and runs epoch, topology, union, rank and replay preparation before one owner advance. The owner reads progress, calls Native once, and adopts returned progress through the catalog CAS. Rank preparation checks the retained completed graph-result row and its counters; Native `_OwnedSource.verify_current` checks worker/source/certificate authority but does not substitute for that exact readiness predicate. [S8, S9]

The pure completion-promotion-and-return candidate still reaches the full Core chain on its **next** call before semantic `recording` or accepted-prefix publication. Therefore the missing-rank-result counterexample directly defeats a broader internal Native loop; it does not by itself prove that this one administrative edge skips the later recording validation. It does change when parent `recording` is visible and removes a distinct Core invocation, progress adoption, stamp comparison and scheduler opportunity from a successful trajectory. Core must decide whether that changed observation is allowed. The scheduling opportunity is not known to be a no-op: finalization readiness checks planner/payload/object state, not a direct Native-terminal predicate. [S8–S10]

The candidate must not become “continue until useful work.” It stops immediately after the one allowed promotion. Leaving the current one-step Core runtime unchanged retains its existing scheduling and custody boundaries; no fairness improvement is established. The candidate supplies no authority for new execution concurrency, an extra retained Session/reader, early slot/connection/storage release, or an inference of idle worker capacity from a retained slot. Fewer administrative returns do not establish a different memory peak. [S8, S9]

## Reported newer CPU observations are not this raw trace

The request reports a completed `d4d7` function-CPU diagnostic with 479 advances, 23.491 s helper elapsed, 18.949 s retained-advance wall / 10.367 s thread CPU, and 482 explicit commit API calls with 8.713 s wall / 0.132 s thread CPU. The equal commit count is consistent with the historical inventory, but these are **different executions and observer scopes**. The request does not supply the raw bytes locally; its supplied raw hash `dd39bd52cbe6d8e5b5e3b56c51a5f24e01fb7c4d79e8e8f72c50759fb74a743c` is recorded as reported, not independently verified here. No fsync, storage-contention or lock-wait attribution follows from the wall/CPU gap. [E3]

The smaller-observer run is explicitly partial: 440 of 479 advances before the cooperative cap; the first two histories completed (232 advances), and correction stopped after 208 of 247. The reported completed-case profile accounts for 2.741 s exclusive thread CPU, including 5,562 canonical calls (0.581 s inclusive at that function boundary) and 405 compression calls (0.440 s inclusive at a separate boundary). Inclusive ancestor/descendant intervals must not be summed; none gives per-candidate-edge cost. The partial raw and accounting hashes are retained as reports in the JSON. No retry, completion inference or throughput comparison is added. [E3]

The historical raw itself has 71 restored hooks, original probe/lane/launcher exit 0, 20 received ordinary handles, zero retained admission counters/frames and removed scratch. Its lane elapsed was 13.399518318998162 s, under its 120-second cooperative cap. That is a historical instrumented observation, not a present timing claim. DBAPI counts cover admitted leaf attempts and explicit commit/rollback methods; implicit context-manager transactions are outside that counter, scripts are not expanded statements, and returned rows are not engine visits. Selected scopes include observer and admission overhead. No new dynamic evidence is needed to establish the completion-discharge gap. [E1]

## Reproduction and evidence labels

Run only the external stdlib extraction against the retained files and exact git objects:

```bash
/workspace/scratch/21c38f9cc56d/riverhog/.venv/bin/python -B /workspace/scratch/21c38f9cc56d/native-granularity-reconcile.py \
  --repo /workspace/scratch/21c38f9cc56d/riverhog \
  --source d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e \
  --historical-reference 8b639901c4dbe157dce904acec718a23f580c6ca \
  --raw /workspace/scratch/21c38f9cc56d/native-phase-observation-r2/run.json \
  --report /workspace/scratch/21c38f9cc56d/native-granularity-inventory.md \
  --output /workspace/scratch/21c38f9cc56d/native-granularity-growth.json
```

In the published packet, `EVIDENCE.md` maps this external script to `.reference/evidence/reconcile_inventory.py` and this report to `.reference/INVENTORY.md`; replay from that checkout with the archived script and explicit `--report .reference/INVENTORY.md`, adjusting the external input/output locations as needed. The original executed command remains in the JSON's `analysis.command` receipt.

The script verifies raw decompression byte identity, literal source/oracle hashes, original receiving receipts, the exact two-file delta and pinned source bytes. It indexes all 479 original control records, extracts all child prepare transitions and copies each selected final-Finish/promotion pair unchanged as JSON data. It performs no product execution. The script's exact bytes/hash/argv and this report's bytes/hash are recorded in the output. “Historical measured” fields are counted from that pinned raw; “reported” fields come only from the supplied comments; “static/proposed” fields are source reasoning or requirements, never fabricated outcomes.

The first offline extraction exited 1 before writing its summary because EntryPreparation's recorded preparation has `before_json` but a null `after_state`. Its original script and schema-error receipt are preserved externally. The corrected extraction uses the actual matching `_apply.state` for each returned transition and requires equality whenever a preparation's `after_state` is present. The corrected extraction exited 0. This was a JSON-record extraction correction; neither attempt imported or ran Native code, and no historical input or receipt was changed.

### Source citations at exact current source

- **S1 — EntryReplay ownership, accounting and transitions:** [_start and _commit](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/_entry_replay.py#L2230-L2316), [child declarations](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/_entry_replay.py#L1131-L1457), [completion dispatch and application](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/_entry_replay.py#L2441-L2497), [whole-advance declaration](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/_entry_replay.py#L2020-L2189), [one-branch dispatcher](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/_entry_replay.py#L2647-L2673).
- **S2 — SemanticFinish:** [declared scope and bounds](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/_semantic_finish.py#L183-L409), [checkpoint/current-row validation](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/_semantic_finish.py#L541-L599), [single-phase prepare](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/_semantic_finish.py#L601-L787), [application/CAS](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/_semantic_finish.py#L789-L846).
- **S3 — Parent checkpoint:** [fresh prefix/read/fence/publication](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/_session_checkpoint.py#L412-L566), [warm-corruption cases](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/tests/test_validation_checkpoint.py#L183-L223).
- **S4 — Session:** [budgets](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/journal_session.py#L66-L75), [progress rendering](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/journal_session.py#L189-L217), [child composition](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/journal_session.py#L293-L697), [terminal/advance/result](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/journal_session.py#L1456-L1585).
- **S5 — Generic semantics:** [bounded walk](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/_generic_semantics.py#L1553-L1649), [separate cleanup phases](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/_generic_semantics.py#L1738-L1796).
- **S6 — Correction:** [global active-row seed and application](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/_correction_projection.py#L429-L585).
- **S7 — Existing Finish tests:** [late scan failure](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/tests/test_semantic_finish.py#L199-L243), [stale prefix and finished worker](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/tests/test_semantic_finish.py#L322-L368), [process death](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/tests/test_semantic_finish.py#L548-L639), [byte boundary](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/tests/test_semantic_finish.py#L936-L954).
- **S8 — Core unit and scheduling:** [guard chain and one owner step](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L2122-L2177), [per-progress scheduling](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L2238-L2251).
- **S9 — Core authority and receiving:** [rank result predicate](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_rank.py#L251-L290), [Native source callbacks](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_validation_owner.py#L402-L438), [one Native call, adoption and failures](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_validation_owner.py#L2029-L2122).
- **S10 — Finalization observation:** [scheduling](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L3618-L3667), [readiness predicate](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L7369-L7384).

### Evidence citations

- **E1 — Exact historical lossless packet:** [published raw](https://github.com/nashspence/riverhog/blob/8b639901c4dbe157dce904acec718a23f580c6ca/.reference/evidence/observation/run.json.gz), [original execution receipt](https://github.com/nashspence/riverhog/blob/8b639901c4dbe157dce904acec718a23f580c6ca/.reference/evidence/observation/execution.json), [original lane](https://github.com/nashspence/riverhog/blob/8b639901c4dbe157dce904acec718a23f580c6ca/.reference/evidence/observation/lane.json), [received phase reconciliation](https://github.com/nashspence/riverhog/blob/8b639901c4dbe157dce904acec718a23f580c6ca/.reference/RECONCILIATION.md).
- **E2 — Reported prefix refinement and retained one-step decision:** [codec refinement](https://github.com/nashspence/riverhog/issues/991#issuecomment-6887528297), [one-step decision](https://github.com/nashspence/riverhog/issues/991#issuecomment-6889902338).
- **E3 — Current request and reported complete/partial CPU diagnostics:** [comment 6890881026](https://github.com/nashspence/riverhog/issues/991#issuecomment-6890881026), preserved locally in `native-granularity-request.md` and `native-granularity-context.json`.
