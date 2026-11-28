# Attached latency experiment: findings and recomputed evidence

Analysis date: 2026-11-27 America/Los_Angeles (2026-11-28 UTC). Scope: inspect the supplied archive, its source, patches, logs and JSON; independently recompute the existing numbers; identify a small useful next production experiment. No remote mutations. No product runtime or unsafe experimental patch was executed.

## Main finding

**The archive contains no newly measured Riverhog publication or marginal-file latency.** Both saved product preflights stopped before the first observation because `mise` was unavailable. Both comparison manifests say `incomplete-no-verdict` and have `samples: []`. There are no product `profile.json` receipts or actual product diagnostic call-count records in the ZIP.

The useful measured evidence is an explicitly separate **SQLite toy model**, not the proposed real-path matrix. It suggests that redundant control representations and transaction granularity deserve focused investigation. It does not establish the share of Riverhog's runtime they consume, that a production-safe simplification already exists, or that 20/50/100 ms per added file is achieved.

The supplied README is candid about these limits. Its later instructions to implement restart/adaptive B/C arms under #994/#1000, and its advisory-only CI instruction, are superseded by the current #1006 direction. They are not new prerequisites.

## Archive and validation facts

Safely inventoried and extracted 23 regular files, 82,139 uncompressed bytes, from a 33,599-byte archive. There were no symlink members or path-traversal entries. Extraction enforced regular-file paths, bounded sizes and destination containment. No archive member was executed before inspection.

Archive SHA256:

`18e804b58bcd21623eba726d2b965ca01e268cae4b89f157d858246ef8794534`

Supporting published evidence: [evidence/experiment.json](evidence/experiment.json) contains sanitized recomputed values, original archive identity, sampled source/patch identity and the original-file inventory. The supplied archive was reviewed locally; this reference analysis does not turn its unsafe proxies into production proposals.

All patch/count-module hashes in both comparison manifests match the attached files. All stored model min/median/max summaries and expected SQL counts recompute exactly. The deterministic terminal digest was independently recomputed from the 9,674 row values and matches every sample:

`95ef7eaf9f07634418aa01aad72bf357c6b88f908ef43163dfc200338754d31c`

After reviewing the entire imported harness and test code, I ran its existing eleven tests. They pass in 0.008 reported seconds. Their scope is synthetic receipt validation, geometry/slope arithmetic, failure rejection and in-memory SQLite API counting in a Python thread. This is harness validation, not product timing or recovery proof. I did not rerun the toy microbenchmark, install the missing product toolchain, or launch the unsafe proxy.

## Source identity and currentness

The harness pins:

| Arm | Actual meaning |
| --- | --- |
| A | Historical instrumented pre-cut baseline `9f366cdfc7e06ea03e2c7ade7f74dcc972af610b`. |
| I | Historical draft PR1001 snapshot `0b25c7c67f7b3ee8787d8d753d4b6d585e8cea1f`. Its variable name `CURRENT` does not make it today's authoritative source. |
| P | I plus parent-mirror bypass and private JournalStore `synchronous=OFF`. |
| M | I plus parent-mirror bypass only. |
| S | I plus private JournalStore `synchronous=OFF` only. |

The script creates local detached worktrees and commits the common observer plus selected proxy changes, so its recorded actual profile-source SHAs differ from the underlying A/I parent pins. The final preflight records A=`5e3f5f0f62900d46e4b6726fcd8d2e31305323ac`, I=`f97394ba3d677b59f7efdb9cf3cd6c8763505c53`, P=`974dbad1f7f49465f1f5603f4a0f80ba1f26a6b9`. The ZIP includes patches and source files, not the corresponding Git objects/full source snapshots. Their complete provenance cannot be reconstructed from archive contents alone, though the patch hashes are internally consistent.

The host recorded for the failed preflight was Linux x86-64, host Python 3.13.5, five reported CPUs. README requests the repository's pinned product toolchain/Python 3.12.3; no product run reached it. This packet establishes no approved shared-runner environment. Reconcile a new experiment with the currently selected authoritative source, rather than treating I as the continuation target after retirement of #994 and PR1001.

## What the proposed matrix actually exercises

The planned workload uses the maintained diagnostic `make profile` collection-publication fixture. It has in-process authenticated HTTP, SQLite, real encryption/publication logic, and an **in-memory ciphertext store**. Append calls are one member at a time. It verifies payload/history with fresh/warm/evicted Native readers after source/upload scratch removal, but no such new run completed in the archive.

Both regimes fix:

```text
shared_root_members = 1
shared_claim_members = 1
retained_journals = 2
required_journals = 1
```

| Member count | Fixed-total regime | Fixed-tiny regime |
| ---: | ---: | ---: |
| 1 | 65,536 B/file; 65,536 B total | 4,096 B/file; 4,096 B total |
| 2 | 32,768 B/file; 65,536 B total | 4,096 B/file; 8,192 B total |
| 4 | 16,384 B/file; 65,536 B total | 4,096 B/file; 16,384 B total |
| 8 | 8,192 B/file; 65,536 B total | 4,096 B/file; 32,768 B total |
| 16 | 4,096 B/file; 65,536 B total | 4,096 B/file; 65,536 B total |

This is not the historical 16 × 65,536-byte workload with shared fanout 16. The two regimes coincide only at their F16 endpoint. Fixed-total payload does not hold archive metadata/history bytes constant. Fixed one-member root fanout means these runs would not establish how shared-history fanout scales.

The observer divides admission-to-result into initialization, append, primary history, shared history, member staging and finalization; it also reports result-to-receiving tail, separate verification readers and fixture preparation. These are useful attribution boundaries. Some primary/shared-history activity is fixture orchestration, and source creation/hashing for expected fixture output occurs before admission. Consequently this fixture's clock is not complete operator ingestion.

It preserves the original progress/milestone file writes and adds observer work. Same-observer pairing reduces one confounder, but does not create an observer-free floor. Optional Python profiling counts nested method/API entries and does not establish physical fsync counts, critical-path time or CPU attribution by individual worker.

## Recomputed toy-model results

Source: `checkpoint_cost_model.py` and `checkpoint_model_results.json`.

The model runs 9,674 deterministic simple semantic inserts. That number was borrowed from an earlier F1-plus-all-readers count, **not publication-only and not one operation per file**. It uses `journal_mode=DELETE`, disables SQLite statement caching with `cached_statements=0`, and runs under `/dev/shm` tmpfs. Each arm has three recorded rounds.

| Model arm | Median wall s | Observed range s | Control SELECTs | Parent updates | Transactions |
| --- | ---: | ---: | ---: | ---: | ---: |
| legacy / FULL | 1.148503 | 1.069803–1.235799 | 58,044 | 9,674 | 9,674 |
| typed / FULL | 0.747317 | 0.746308–0.833648 | 58,044 | 9,674 | 9,674 |
| volatile / FULL | 0.470283 | 0.463843–0.528076 | 38,696 | 1 | 9,674 |
| volatile / OFF | 0.472331 | 0.445066–0.482715 | 38,696 | 1 | 9,674 |
| grouped / OFF | 0.187004 | 0.186286–0.187596 | 38,696 | 1 | 152 |

All five preserve the model's final 9,674 rows and digest. “Typed” here largely replaces repeated JSON decode/canonicalization comparisons with string equality; it is not a demonstration of the complete product's proposed typed execution architecture.

Within this model only:

- Typed/FULL's median is 34.9% below legacy/FULL.
- Volatile/FULL's median is another 37.1% below typed/FULL; this changes parent reads, parent updates and associated serialization together.
- Grouping up to 64 semantic units per transaction reduces commits from 9,674 to `ceil(9674/64) = 152`, a 98.43% count reduction. Grouped/OFF's median is 60.4% below volatile/OFF.
- The combined grouped/OFF median is 6.14 times faster than legacy/FULL, an 83.7% wall-time reduction. This is a toy-model contrast, not a measured product improvement.

**Turning sync off has no stable demonstrated benefit in this dataset.** Volatile/FULL to volatile/OFF saves +55.746 ms, −18.872 ms and +25.217 ms in the three paired rounds. The paired median saving is +25.217 ms, while the difference between separately computed wall medians is −2.047 ms (OFF's median is 0.44% slower). These descriptive statistics disagree because the tiny series varies. They do not justify either “sync is free” or “sync dominates.”

Process CPU is approximately 98.7–100.0% of wall time across samples. Together with tmpfs, this indicates this recorded model is principally exposing local software work, not characterizing durable-media flush latency. The JSON explicitly records two earlier overlay-filesystem attempts that reached tool deadlines without complete records; their raw attempts are not supplied. The completed tmpfs series cannot replace those absent storage results.

The model omits full Native semantics, immutable archive authority, authenticated/encrypted publication, canonical history/closures, real provider effects, concurrency, crash/restart outcomes, original worker receiving and actual physical storage durability. Its deterministic row digest proves the toy result only. Its per-unit microseconds must never be relabeled per-file milliseconds.

## Patches and causal strength

`mirror_only.patch` and the corresponding portion of `fast_path_proxy.patch` act on `packages/riverhog-provenance/src/riverhog_provenance/_session_checkpoint.py`:

- `ValidationLedger._read` returns live owner state after prefix checks instead of reading the parent mirror on intermediate steps.
- It refuses a cold nonterminal ledger adoption explicitly.
- `_publish` retains an initial mirror and a terminal update while skipping intermediate parent updates.

This is a useful counterfactual to locate redundant representation cost, but it deliberately removes general resume behavior and bypasses intermediate persisted-parent checks. A production design must establish owner/epoch lifetime and stale-work fencing independently; prefix equality alone is not blanket proof that all omitted authority checks are redundant.

`sync_only.patch` changes new private JournalStore initialization to `PRAGMA synchronous=OFF` even for retained workspaces. It does not change Core/catalog sync, batch real product transactions, remove all corpus/control mirrors, implement generation adoption or prove a safe disposable-store lifecycle. A “retained” flag paired with weaker sync cannot silently become the production contract.

P combines these edits and therefore would not isolate their contributions. M/S versus I are intended ablations, but none ran. A versus P additionally crosses a historical product revision, so it cannot attribute effects solely to the proxy. The README correctly says interaction prevents simply adding M and S savings.

The strongest valid causal claim is: changing these model representations/transaction patterns changes runtime for this model. The strongest product claim is a **source-based hypothesis**: repeated parent JSON/SQL work and per-microstep transactions may be accidental costs worth measuring on the actual selected path. There is no measured ranking of Riverhog hotspots in the new attachment.

## Interpreting the requested marginal-cost rubric

For a constant marginal cost m milliseconds per added file, 1,000 added files contribute `1000 × m / 1000 = m` seconds. Add fixed setup, actual payload/storage/network work and any superlinear growth separately.

| Marginal cost | Conditional increment for 1,000 files | Interpretation supplied/implied by the request |
| ---: | ---: | --- |
| 20 ms/file | 20 s | Promising |
| 50 ms/file | 50 s | 50 ms inferred arithmetically; also the attachment’s working objective |
| 100 ms/file | 100 s = 1m 40s | Further optimization |
| 500 ms/file | 500 s = 8m 20s | Major architecture work |
| 1,000 ms/file | 1,000 s = 16m 40s | Unacceptable |
| 5,000 ms/file | 5,000 s = 83m 20s | Fundamental redesign |

The inferred 50/500 ms entries repair the units, not measured results. These bands are a decision rubric for useful engineering outcomes. The packet's default 50 ms and illustrative 1-second fixed budget are local experiment parameters; they are not independently approved production limits. Likewise historical `241.949 / 16 = 15.1218125` seconds/file is an average including fixed work, not a marginal slope. Subtracting unrelated historical F1 and F16 timings would be invalid.

For observations at matching conditions, compute `m = 1000 × (T_hi − T_lo)/(F_hi − F_lo)` separately for result and full receiving. Report fixed cost and the actual pair/repeats. A slope from F8→F16 is a finite difference, not proof of linearity through F1000. Negative or unstable slopes indicate measurement limitations. The supplied summary's “remaining improvement” fields use result slope alone even though its verdict also considers receiving; any reuse should show receiving's remaining cost separately to avoid hiding a mandatory tail.

## Minimal useful production follow-up

The next step should remove or validate one concrete cost, not produce another broad framework or revive #994.

1. **Select authoritative current source and one actual tiny-file publication path.** Reuse existing lifecycle entrypoints and independent output verification on a real supported local backend with declared normal crypto/authentication and storage settings. Keep setup/fixture work explicit. The historical in-memory diagnostic fixture may still help isolate code, but its result must remain labeled that way.
2. **Run the smallest matching baseline first.** F1/F2 is a start; F4 can distinguish fixed collection cost from a growing per-file phase if the first pair is noisy. Record result and required receiving with coarse phase times. Expand to F8/F16 or a second payload regime only to answer a concrete unresolved growth/byte-cost question. Keep all failed observations. Do not schedule the full five-arm/two-regime matrix by default.
3. **Make one justified simplification.** Candidate seams are repeated parent serialization/mirroring, per-unit SQL crossings/commits for rebuildable progress, repeatedly revalidating immutable data within a proved owner lifetime, and actual critical-path polling. Select the dominant measured phase first. Batch at the smallest safe meaningful completion boundary; do not equate bounded scheduler work with durable checkpoints. Preserve fresh authorization and receipts for real external effects.
4. **Compare the exact changed source with that baseline under the same conditions.** Use existing correctness/growth/interruption/receiving checks for the affected owner and a small repeated timing comparison. Show added-file cost before/after, fixed cost, required tail, removed calls/transactions where relevant, and the actual code removed. A fast unsafe proxy is a diagnostic signal, not a shippable change.
5. **Stop or proceed based on the user’s bands.** If local overhead remains hundreds/thousands of milliseconds per added tiny file, identify the remaining dominant work and simplify the execution path; percentage improvement alone is inadequate. If it reaches tens of milliseconds while preserving semantics, then spend on a larger or backend/platform-specific trial. Concrete complete-operation budget approval and current all#68/all#64 acceptance remain explicit steps when the final operation boundary and useful numeric limit are chosen.

Useful permanent output is smaller production machinery plus a meaningful owner regression and an honest cost comparison. This attachment does not support adding a checkpoint governor, a mandatory adaptive/restart program, a per-function census, or an elaborate new benchmarking subsystem.

