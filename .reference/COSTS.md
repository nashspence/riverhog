# Native phase cost: derived receipt summary

Derived offline from `native-phase-observation-r2/run.json`, SHA-256 `019236fdc3b227d49753b54e79915b62d45cf44e788283d3843411611b33c00b`. This is one instrumented observation on clean `3b47cbba770c31eb1487b50f7e3b16533c10e827`. No product, fixture, test, or profile was rerun to produce this summary.

## Receiving and workload

The actual workload passed: probe, maintained lane, wrapper and launcher reported exit 0. All 71 hooks were restored; all 20 retained ordinary-validation handles, retained construction frames and admission resources were received. Owned scratch and the outer temporary root were removed. The first attempt remains a preserved preflight failure (exit 1, no Native imports/work or retained advances); its only corrected source line accepts the platform suffix on pinned `uv 0.11.24`.

| History | Frames | Literal bytes | Retained advances | `_start` / ledger loads within advances |
|---|---:|---:|---:|---:|
| initial | 1 | 4,913 | 107 | 210 / 210 |
| checkpoint | 2 | 5,903 | 125 | 244 / 244 |
| correction | 2 | 6,193 | 247 | 488 / 488 |

All three retained results matched ordinary `validate_journal` materialization and exact source bytes/digests. Each final graph has six effective assertions; the correction retires one assertion. Defaults remained rows **64**, source octets **65,536**, work octets **33,619,968**. No graph certificate was supplied. Fixture construction and the three ordinary validation oracles perform additional real Native work outside the 479 retained-advance counter.

## Timing scope and disjoint partition

The maintained lane took **13.399518 s**. The probe’s pre-receipt interval was 12.992227 s; the outer launcher took 13.771764 s. These intervals nest. The cooperative 120-second work cap was not reached; it is a safe-boundary stop, not a guarantee that an entered call or cleanup can be force-finished by 120 seconds.

The four retained stages—setup, advances, terminal materialization and original cleanup—sum to **5.529906 s wall / 3.228685 s process CPU**. The table below partitions their selected scopes using thread CPU and same-thread direct-child subtraction. All 19 recorded stage partitions reconcile exactly to their root scope; small differences from outer stage clocks are stage-boundary measurement work.

| Selected owner category | Exclusive thread CPU s | Exclusive wall s |
|---|---:|---:|
| Parent ledger controls, excluding child scopes | 0.944522 | 0.947917 |
| Admitted DBAPI execution | 0.815808 | 1.267360 |
| JournalValidationSession residual | 0.302352 | 0.304244 |
| GenericSemantics, excluding child scopes | 0.253902 | 0.254534 |
| Explicit DBAPI commit / rollback | 0.198315 | 2.026572 |
| EntryReplay residual / orchestration | 0.162505 | 0.165273 |
| Admitted DBAPI fetch / iteration | 0.133231 | 0.132260 |
| Stage dispatch / unscoped work / bookkeeping | 0.114256 | 0.127762 |
| Semantic completion | 0.103444 | 0.103629 |
| Prefix preparation and publication | 0.082452 | 0.082489 |
| Entry preparation and assertion staging | 0.058261 | 0.058501 |
| EntryReplay._start | 0.029826 | 0.029784 |
| Source provider and retained framing/input | 0.012152 | 0.011866 |
| Correction target / projection owners | 0.012032 | 0.012066 |
| OnlineDag planning | 0.005096 | 0.005117 |
| **Captured retained total** | **3.228154** | **5.529375** |

“Residual” includes unwrapped work and recorder effects; it does not mean removable overhead. Explicit commit scopes have a large wall/CPU difference, but these clocks do not identify fsync, lock contention, scheduler delay, or another wait. No uninstrumented baseline or speedup was measured.

## Control repetition and SQL observations

There are **463 double-start turns and 16 single-start turns**. All captured checkpoint fields match across each double load, and both `_start` calls reset to the same observed reserved-byte value. All 942 calls install their supplied budget object. `_start` totals **0.029826 s thread CPU**, about **0.965%** of captured advance-stage thread CPU. Its duplication is measured, but it is a small part of this instrumented cost and does not justify removing an authority fence.

Within advances, ledger `load` / `_prefix` / `_fence` / `_publish` exclusive residual CPU sums to **0.941663 s**, with DBAPI children charged separately above. The inclusive load/fence/publication intervals total 1.383861 s CPU; they include nested prefix and SQL work and must not be added to the partition.

| Retained DBAPI observation | Calls | Returned rows / completion |
|---|---:|---:|
| `execute` | 9,524 | All returned |
| `executescript` | 12 | All returned; scripts not expanded |
| `fetchone` | 7,453 | 6,861 rows |
| `fetchall` | 254 | 84 rows |
| `fetchmany` | 24 | 18 rows |
| Iterator `__next__` | 58 | 36 rows; 22 normal `StopIteration` |
| Explicit `commit` | 497 | All returned |
| `executemany` / explicit `rollback` | 0 / 0 | No observed invocation |

The 497 explicit commit API calls comprise 15 during setup and 482 during retained advances. The latter include 479 parent checkpoint publications plus three terminal `JournalStore.freeze()` commit method calls. Commit counts therefore do not equal parent transition counts.

No raised exception family was observed in the retained scopes. These are admitted DBAPI leaf attempts, including admission checks; they are not engine-statement or row-visit counts. Connection shortcuts are counted once. Implicit context-manager commits/rollbacks are outside the explicit transaction counter. The JSON companion contains exact counts by case, stage, parent phase, calling owner and SQL verb, and the complete exclusive category mapping.

## Coverage and remaining gaps

The added checkpoint changes 107 advances to 125 while keeping the same six effective assertions. The correction reaches 247 advances and repeats generic/row/completion work for its accepted effective projection. This is a tiny route contrast, not a growth-rate result. Child load/prepare/application states were captured from actual returns; OnlineDag planning recorded retirement/replacement activity, including explicit terminal `None` results, but no nonzero adjacency visits.

Source windows returned 4,913 / 6,893 / 7,473 bytes; direct exact frame reads returned 4,911 / 5,899 / 6,189 bytes. The window totals exceed literal input length by 0 / 990 / 1,280 bytes. `read_window` delegates to `read_exact`; its nested exact delivery is the same read and must not be added twice. These are byte-return observations, not memory measurements.

Row-child boundary methods were observed through the same `GenericSemantics` hooks: initial and checkpoint each have six generic plus four row admissions, with 18 generic and 12 row prepare/application calls. Correction has twelve generic plus eight row admissions, with 36 generic and 24 row prepare/application calls. Their loads, admission and result calls are also recorded. Missing detail concerns internal program steps and row-specific handlers, not unobserved row-child preparation.

Uninstrumented internals remain inside their nearest selected owner: `_checkpoint_progress` encoding/hash work inside Session residual; GenericSemantics private loads, program execution, row-specific handlers, projected validation and SemanticEdge bodies inside semantic residual (apart from nested DBAPI and selected graph plans); and other constructors, canonicalization, parsing and Python loops. The probe does not isolate all semantic primitive work, database wait or observer tax. Fixture/oracle raw SQLite is outside admitted-SQL counters. It executes no Core publication, API coordination, worker scheduling or SQLAlchemy catalog workload, so the historical **418 raised Core catalog attempts remain unclassified**.

Complete derived tables: [cost-summary.json](evidence/cost-summary.json) (SHA-256 `c8ca95297d7921771ffda38328ab43a0b4e1ba8b50a32384b898c10933d927b8`). Original receipts remain unchanged.
