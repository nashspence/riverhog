# Numeric evaluation: finite source-bound results

## Recommendation and exact source

Receive the narrow declaration-metadata reuse for normal integration review. The numeric reader delta is **26 additions / 6 removals** against the accepted lookup reader. Retain every live dimension validation and numeric evaluation. Once-per-sample validation hoisting was excluded because callback-bearing mappings can change observations between bounds; maintained controls demonstrate that those changes remain observable and rejecting.

| Role | Exact identity |
| --- | --- |
| Explicitly requested integration base | `824383a8a8143aac033f86dd1562a5979f6ec4da` |
| Accepted public reader/test overlay | `0e9bef9445213aadadb7218fc6e268cc324f35f3` |
| Clean tested implementation | `c4bf7532b7a0e91ac9ec384ff5893e81eae7bf01` |
| Tested implementation tree | `058666d90ef2340b3401737c7357fb8c9f77eb24` |
| Final handoff | Pinned by the owning issue; only `.reference` is appended after the tested implementation |

The baseline includes the accepted parsed unit-lookup maps; it is not the pre-lookup reader at the integration base. The reported local integration commit `b47858a54fadd3f1ddcd7aeff39d920405c714ec` was not inspected or treated as published source.

The [published-source re-audit](evidence/published-source-reaudit.json) still finds `codex/964-session-owner` and PR #979 at `824383a`. All twelve inspected reader/canonical-owner/guidance/environment/validation-owner paths match that base. No later published checkpoint is available. Its requested audit therefore remains a receiving limitation; neither the private local commit nor a future checkpoint is qualified by these results. Reconcile it before integration.

Relative to the integration base, three executable/test paths are carried: the reader, the already accepted lookup tests, and the new numeric tests. Relative to the accepted overlay, the new proposal changes only the reader and adds the numeric tests. [Source integrity](evidence/source-integrity.json) pins all three blobs and the evidence-only append boundary.

| File identity | SHA-256 |
| --- | --- |
| Accepted baseline reader | `3929511343a7817e435427ef935a5f85b2c7ee85d9ee1baed51c217da3fe0424` |
| Proposed reader | `a6fa11e73a3886dbf950bba34e310ab27857c5e87af6ca77bb6cad756a059f40` |
| Unchanged accepted lookup tests | `2650d46fa1e23137bf1958adc8dffe4f02e3da83ffbb2e129b8b9984bb724870` |
| New numeric tests | `8e12500aeccd012920c8333a6fb4ae36359dc0ce99471ab60c6efb9c0aae0226` |

[The structural audit](evidence/source-structure-audit.json) finds changes only in `_Context`, `_projection_contexts`, and `_evaluate`. The other 21 top-level definitions retain both their complete source fragments and ASTs. Exports and all inherited metadata, expression, graph, depth, and per-root sample limits remain. Removing only the new metadata routing reproduces the complete accepted reader AST.

## Retained inputs and scope

The original projection contains **100 declarations, 2,867 dimension entries, and 3,383 bounds**, with **4,512,672 canonical bytes**, SHA-256 `f8eae73e293eac5e9ed7dcf01d706d9176d4f198e13ef61203d06d0df41c0ef1`.

The samples contain **122 independent roots and 1,078 total sample nodes across roots and descendants**, SHA-256 `a9e69163ba9aac6c5cefd4f582c609997217421528840694449f24007f6c2b20`. These are the prior actual three-turn samples. The **46 ordered groups** come from the prior helper-replay receipt, SHA-256 `b62367b740cf428e5c7d39f8233237e63ca7ed226f65951b926de9e91cb6a844`. Their complete reachable projections contain **829 declaration occurrences** in aggregate.

The literal sample inventory has 19,024 dimension fields and 30,745 metric fields; runtime repeated scans are counted separately. All group hashes, declaration rows, root tokens 0–121, node inventories, and input bytes were preserved. No new `project_bindings` call, actual caller turn, product operation, publication, or journal fixture occurs in the comparison.

Both exact reader modules run on one clean proposal checkout with the same pinned environment and unchanged canonical owner. The four passes are diagnostic baseline, diagnostic proposal, unmodified baseline, and unmodified proposal. Each independently decodes identical inputs before timing; each of its 46 public calls parses its projection freshly.

## Separate unmodified observations

| Complete retained public-reader workload | Baseline | Proposal |
| --- | ---: | ---: |
| Wall time | 11.226256326 s | 8.105117364 s |
| Process CPU | 11.198141663 s | 8.089405463 s |

This pair observed **27.80% less wall time** and **27.76% less process CPU**. It is one observation per reader in fixed order, not a statistical estimate, timing gate, or product-throughput claim. Interpreter, allocator, cache, and host scheduling effects remain possible.

The timer includes complete public calls: parsing, context/index construction, typed/unit/graph checks, numeric validation, declaration digests, actual delegates, and composition. Both readers use identical minimal result-list collection after each call. Imports, module construction, input preparation, behavioral comparisons, diagnostics, input-equality checks, and reporting remain outside these timers.

This pair is not compared numerically with the integration agent's separate 4.28-second observation or 46.53-second cProfile pass. Those are different executions and measurement conditions. No whole-path or production latency is inferred.

## Diagnostic reconstruction work

Temporary AST copies wrap actual construction/predicate results and return the original objects/results. Original branches and lazy generator consumption remain. Removing the wrappers reproduces each original AST exactly, with 23 baseline and 28 proposal sites accounted for. Extra diagnostic snapshots inspect only valid retained plain JSON; callback cases use unmodified readers.

| Numeric declaration-metadata construction | Baseline builds / cumulative entries | Proposal builds / cumulative entries |
| --- | ---: | ---: |
| Expected dimension-name set inside `_evaluate` | 56,955 / 6,271,005 | 0 / 0 |
| Metric-to-bound map inside `_evaluate` | 56,955 / 14,867,376 | 0 / 0 |
| Fresh parsed-context numeric-name frozenset | 0 / 0 | 829 / 16,574 |
| **Total at these changed sites** | **113,910 / 21,138,381** | **829 / 16,574** |

The proposal makes **56,955 bound-alias uses** without copying bound entries. It performs 16,574 dimension-name and 24,801 metric-name eligibility checks across 829 fresh contexts. Both readers retain the existing parsed dimension and bound maps: 829 / 16,574 entries and 829 / 24,801 entries respectively. Those unchanged constructions are not counted as savings.

Every live supplied dimension-name set is still constructed: **56,955 builds / 6,271,005 entries** in each reader. Every actual dimension predicate is still evaluated: **6,271,005 inspections**, with **56,955 completed exhaustive scans**. Predicate inspections use a different counting convention from the integration's cProfile generator-resumption observation; they are not presented as an independent measurement of resumes.

These are cumulative work counts, not simultaneously retained entries, allocated bytes, RSS, or peak memory. Each eligible parsed context gains one frozen name set and two reference slots; the bound reference aliases its existing map. Source contexts gain the optional slots but retain reconstruction. Projection-only calls also pay the new eligible metadata construction; no separate projection-only/source-bound timing or memory profile was run.

## Required work preserved

Both diagnostic records have identical complete required-work dictionaries, all 14 ordered transcript hashes, and every receiving/edge inventory.

| Required work | Baseline and proposal |
| --- | ---: |
| Public calls / fresh parses | 46 |
| Fresh parsed contexts | 829 |
| Parsed `Expression` objects | 333,837 |
| Unit visits and returns | 492,075 |
| Numeric evaluations and returns | 56,955 |
| Nonrecursive numeric evaluations / per-bound obligations | 30,745 / 30,745 |
| Live dimension-value predicate inspections | 6,271,005 |
| Numeric-expression visits and returns | 727,504 |
| Composition visits and returns | 398,336 |
| Independent root occurrences | 122 |
| Total successful sample checks | 1,078 |
| Declaration canonical-serialization calls | 1,078 |

The unit/delegate inventory has 23,113 visits across 1,113 keys; numeric delegates have 26,210 visits across 900 keys; composition has 11,685 delegate visits across 796 keys. All match. Numeric argument maps retain 26,210 builds / 223,694 entries; composition argument maps retain 11,685 builds / 136,293 entries.

Canonical calls returned 36,289,880 bytes in aggregate in each reader. The canonical owner is byte-identical to the accepted baseline's owner. Recursive preparation is not separately instrumented, and no additive CPU allocation to parsing or canonical work is claimed. Those semantics remain untouched.

The diagnostic intervals were **41.917870341 / 41.585308732 seconds wall** and **41.806171967 / 41.511901294 seconds CPU**, baseline/proposal. These include substantial instrumentation overhead. They are retained as diagnostic receipts and are not used for the unmodified performance comparison.

## Adversarial and focused verification

Unmodified readers matched **96 JSON cases / 174 public actions per reader**: 67 accepted lookup cases and 29 new numeric cases. Exact return/rejection class/message, input preservation, and same-projection identity across actions matched. Raw JSON snapshots preserve malformed floats and large integers; canonical serialization does not normalize adversarial inputs.

All **15 typed callback/source controls** also matched literal outcomes. They intentionally mutate observations or source bounds; their obligations are exact public behavior and restored source state, rather than immutability of deliberately mutated local observations. They cover mutation between bounds, source replacement/removal during value scans, child source replacement, custom metadata hashes, accepted string subclasses, observation comparisons, error precedence, and freshness across calls.

The new maintained file has **44 cases**: 29 JSON and 15 typed controls. It covers shared-child arguments in both orders, late invalid roots/children, bool/float/negative/missing/extra observations, bound relations, integer/logarithm boundaries, and eager zero-product/zero-delegate evaluation. Prior lookup cases retain unit/relation/ownership/cycle/depth/unused-metadata coverage. Existing reader-budget tests remain unchanged.

On clean `c4bf7532b7a0e91ac9ec384ff5893e81eae7bf01`, one selection passed **85 tests**: 44 new numeric, 36 existing growth-declaration, and 5 existing bound tests. No broad unit-suite rerun or actual caller fixture was included.

| Clean-source check | Original exit/result | Maintained lane elapsed |
| --- | --- | ---: |
| Focused pytest | 0; 85 passed | 29.540720 s |
| Repository Ruff | 0; passed | 0.603539 s |
| Repository format | 0; passed | 0.400034 s |
| Repository REUSE | 0; passed | 1.651426 s |
| Maintained mypy | 0; passed | 54.471844 s |

The controller recorded 85 passing setup/call/teardown sequences and a 26.554653-second pytest interval. The driver checked clean source before/after every gate and after cleanup. Pins matched Python 3.12.3, uv 0.11.24, pytest 9.1.1 and locked plugins, Ruff 0.16.0, REUSE 6.2.0, and mypy 2.3.0. These were direct pinned commands; `mise` was unavailable. Driver elapsed was 88.694276 seconds. See [original verification evidence](evidence/verification/).

## Original execution, retained bytes, and limits

The comparison ran once without retry. Original child, observed wrapper, and lane exits were **0**. Source was exact and clean at both boundaries, all pinned files and unmodified modules remained intact, scratch was removed, and the empty external TMPDIR was removed after wrapper join. Whole lane elapsed was **124.227802373 seconds**, run ID `8673aaa3514d46c0b6047c5383ab2e19`. This includes preparation, behavioral checks, instrumentation, all four passes, reporting, and cleanup; it is not product timing. Verification and profiling ran sequentially.

| Original artifact | SHA-256 |
| --- | --- |
| Full comparison run | `994f1e49a1c2802c97f741323276879d344b784894f4741e95abf61a9f684494` |
| Execution receipt | `4cffe749e291d2330ad29243710e7247ab8c0d72f5d3149c8c6151a73abb6b87` |
| Frozen comparison script | `76c0bf8282daf4315132c807de5807879231b3aeae47c4bd7650ec5e7df1f3d5` |
| Adversarial case document | `89cd7224552256b42c1768baac8eeb87a8f101ab2bd3feef150f6c53f0f51fbf` |
| Receiving-input manifest | `fdecfbd5a4262d8f64e866ebeb2103fafe71db16efbd1055d2875d9bafbf0c9b` |
| Completed verification run | `5f8b52372e403b26bb7e8c1fcf00db05e79625f820e5305fcc105997e44c3706` |

Large original inputs/receipts are losslessly compressed as gzip/base64 text. [VERIFY.md](VERIFY.md) restores exact bytes and checks lengths/hashes. [The compact summary](evidence/profile/summary.json) is derived navigation only; full original inventories and transcripts remain retained. [SHA256SUMS](SHA256SUMS) covers the reference packet except itself. These storage choices create no new acceptance format, cache, or source authority.

No accepted-result cache, numeric-result cache, global context, weakened check, new limit, or performance gate is introduced. This does not repair or qualify the incomplete journal census or its missing independent-root receiving. Full CI, future checkpoint qualification, whole-path growth, terminal/restart/composed proof, production publication cost, and underutilized parallelism remain with their owning work. No new journal census, product publication, broad unit suite, or 1,000+ workload ran.
