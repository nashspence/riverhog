# Finite source-bound results

## Recommendation and source relationship

The finite evidence supports receiving the **two fresh parsed-context lookup maps at the three existing unit-derivation lookup sites**. The proposed reader edit is 17 additions and 4 removals. It reduces repeated metadata-map construction while retaining every measured expression visit and every existing parser, graph, edge, numeric, digest, and sample check.

| Role | Exact source |
| --- | --- |
| Previously issue-authorized audited base | `6e80898a793532beff7a1718effc0618e2472d58` |
| Later inspected queued PR #979 source | `824383a8a8143aac033f86dd1562a5979f6ec4da` |
| Clean tested implementation | `e4f6ccfc42d90cb69791ea1c507b6c79d21edc47` |
| Tested implementation tree | `29a84c2ecd5bfdd253ea5307ba3aa9ee3ef8ed1b` |
| Final handoff | Pinned by the owning issue; only `.reference` is appended after the tested implementation |

The baseline reader file, repository guidance, and locked environment are identical at the audited base and the inspected queued source. Current main was inspected at `209481c189f7387557a85e1c249184642816e103`; its older reader budgets are not the baseline of this experiment. No result here qualifies any of those integration/main sources or the separate compose correction.

Only the growth-contract reader and a new owning test file are proposed executable/test changes. Both retain their tested bytes in the final handoff. [Source integrity](evidence/source-integrity.json) records their blob/content identities. [The source audit](evidence/source-audit.json) verifies that only `_Context`, `_expression_unit`, and `_projection_contexts` changed structurally; the public wrappers, expression parser, source context, numeric evaluation, sample validation, and public typed declaration classes retain their original syntax trees. Public export names also match in the independent reader comparison.

| File | SHA-256 |
| --- | --- |
| Exact baseline reader | `1adc9766c8d0c97cf97ce926695e6c38dc7d50458725e8947b57bfc94c90e376` |
| Proposed reader | `3929511343a7817e435427ef935a5f85b2c7ee85d9ee1baed51c217da3fe0424` |
| New owning tests | `2650d46fa1e23137bf1958adc8dffe4f02e3da83ffbb2e129b8b9984bb724870` |

## Baseline input audit

One public caller/unit projection construction on exact clean baseline source matched the retained projection's **4,512,672 canonical bytes**, SHA-256 `f8eae73e293eac5e9ed7dcf01d706d9176d4f198e13ef61203d06d0df41c0ef1`.

The union contains **100 declarations, 2,867 dimension entries, and 3,383 bound entries**. All 100 callable/binding/declaration/delegate identities remained unchanged. Declaration digests matched for all 122 independent roots and all 1,078 total sample nodes across roots and descendants. This audit verifies source/input identity; sample numeric/composition acceptance is supplied by the later fresh-reader replay.

Original child, maintained wrapper, and lane exits were **0**. The exact baseline remained clean, metadata scratch was removed, and external TMPDIR was empty. One complete `project_bindings` call measured **1.626364 seconds wall / 1.621734 seconds process CPU**; whole lane elapsed was **9.395214 seconds**. This audit includes projection construction, unlike the paired `validate_projection` workload below. Its original [run](evidence/metadata-audit-run.json), [lane](evidence/metadata-audit-lane.json), log, script, and reader bytes are retained.

No product operation, actual caller turn, or physical fixture was executed for this follow-up. The samples come from the previously received three-turn capture, with original sample-document SHA-256 `a9e69163ba9aac6c5cefd4f582c609997217421528840694449f24007f6c2b20`. The exact 46-group order comes from the pinned prior replay receipt `b62367b740cf428e5c7d39f8233237e63ca7ed226f65951b926de9e91cb6a844`.

## Separate unmodified-reader observations

Both readers run on the **same clean proposal checkout and environment**. The baseline module is compiled from the exact archived baseline file; the proposed module is compiled from the checkout's unmodified reader file. This compares those two reader implementations, not whole baseline/proposal repository checkouts.

| Complete public workload | Baseline wall | Proposed wall | Baseline process CPU | Proposed process CPU |
| --- | ---: | ---: | ---: | ---: |
| Full 100-row `validate_projection` | 1.169515453 s | 0.525936273 s | 1.169467810 s | 0.525934672 s |
| All 46 ordered execution-reader groups | 13.071331886 s | 8.171519613 s | 13.053293192 s | 8.159329019 s |

The observations show **55.03% less wall time** for full projection validation and **37.49% less wall time** for the retained execution-reader replay. There is one elapsed observation per reader/workload, with fixed baseline-then-proposed order; these are not statistical estimates or numerical acceptance gates.

The timed scope includes the complete public calls: fresh parsing, context and map construction, graph/ownership checks, unit validation, and—where supplied—declaration digests, numeric bounds, nested samples, and actual composition. Imports, module construction, input decoding/copying, adversarial cases, diagnostic instrumentation, equality checks, and report writing are outside those intervals. No product bodies or primitive product observers execute. These figures therefore do not explain product publication, compose execution, concurrency, or whole-path throughput.

## Diagnostic construction accounting

Separate temporary diagnostic copies wrap the **actual dictionary-comprehension results**, count constructions and returned entries, and return the identical dictionary objects. The context wrapper similarly returns its original object. Removing the diagnostic wrappers reproduces the original AST exactly. Unmodified modules, rather than these copies, supplied the elapsed observations above.

### Full 100-row projection

| Lookup site | Baseline builds / entries | Proposed builds / entries |
| --- | ---: | ---: |
| Current dimensions inside `_expression_unit` | 68,785 / 16,135,352 | 0 / 0 |
| Child dimensions inside `_expression_unit` | 2,875 / 26,308 | 0 / 0 |
| Child bounds inside `_expression_unit` | 2,875 / 54,483 | 0 / 0 |
| Fresh parsed-context dimensions | 0 / 0 | 100 / 2,867 |
| Fresh parsed-context bounds | 0 / 0 | 100 / 3,383 |
| **Total** | **74,535 / 16,216,143** | **200 / 6,250** |

The measured baseline entry counts exactly reproduce the integration agent's source-based accounting.

### All 46 execution-reader groups

| Lookup site | Baseline builds / entries | Proposed builds / entries |
| --- | ---: | ---: |
| Current dimensions inside `_expression_unit` | 492,075 / 100,679,251 | 0 / 0 |
| Child dimensions inside `_expression_unit` | 23,113 / 199,239 | 0 / 0 |
| Child bounds inside `_expression_unit` | 23,113 / 413,304 | 0 / 0 |
| Fresh parsed-context dimensions | 0 / 0 | 829 / 16,574 |
| Fresh parsed-context bounds | 0 / 0 | 829 / 24,801 |
| **Total** | **538,301 / 101,291,794** | **1,658 / 41,375** |

The entry totals are cumulative across constructions/calls. They are not counts of simultaneously live entries, allocated bytes, RSS, or peak memory. The change retains two lookup maps alongside each parsed declaration for its context lifetime, proportional to that declaration's dimensions/bounds; source contexts gain the two optional slots but retain their old reconstruction path. No resident-memory reduction is claimed.

### Validation work retained

| Diagnostic inventory | Full projection, both readers | Execution replay, both readers |
| --- | ---: | ---: |
| Fresh compilation calls | 1 | 46 |
| Parsed contexts | 100 | 829 |
| Unit-derivation entries and returns | 68,785 | 492,075 |
| Delegate-expression visits | 2,875 | 23,113 |
| Independent execution-reader root occurrences | — | **122** |
| Successful sample-node checks | — | **1,078** |

For each workload, both readers have identical ordered unit-entry/return hashes, context inventories, and counts for all 1,113 parent/tag/child/metric/relation combinations. The original 46 group order, root tokens, per-declaration node inventory, and supplied canonical projection/sample bytes remain intact. Each public call parses its own complete reachable projection freshly. Candidate fallback map constructions are zero on these parsed-context workloads; no semantic traversal or independent compilation is removed.

The full diagnostic intervals were 1.559688 / 0.772090 seconds wall for baseline/proposed projection validation and 15.914406 / 10.300368 seconds for baseline/proposed execution replay. These include diagnostic overhead and are separately labeled in the original record. They are not substituted for the unmodified observations.

## Adversarial equivalence and maintained tests

The separate differential comparison ran **67 JSON cases and 124 public actions per reader**. All exact acceptance/rejection outcomes, exception classes/messages, and input-preservation results match. Mutation sequences retain the same projection object across calls. The archive and snapshots preserve malformed float types such as `3.0`, so serialization cannot turn a rejecting input into an integer. This comparison ran outside the performance windows. See [behavior-equivalence.json](evidence/profile/behavior-equivalence.json).

The new maintained file has **71 cases**: those 67 serialized cases, three source-mutation cases, and one exact-owner/alias case. It covers context-specific names and units, all parent/child relation combinations, both shared-child bound orders, argument relation/unit checks, metric/multiplicity errors, actual delegate counts and arguments, unused invalid metadata, unresolved/cyclic/deep edges, ownership, fresh mutations/digests, zero calls, and late invalid roots/children. Primitive expected values are literal. Source mutations are restored in `finally`. Existing metadata and per-root resource-budget tests remain unchanged and were run.

On clean `e4f6ccf`, the six-file focused selection produced **193 passing tests, 1 deselected**, including all 71 new cases. The excluded case is the actual caller fixture: this finite follow-up uses already received actual samples and adds no product trial. No maintained test or integration gate was removed.

| Clean-source check | Original exit/result | Maintained lane elapsed |
| --- | --- | ---: |
| Focused pytest selection | 0; 193 passed, 1 deselected | 30.937784 s |
| Repository Ruff check | 0; passed | 0.612505 s |
| Repository Ruff format check | 0; passed | 0.402523 s |
| Repository REUSE lint | 0; passed | 1.543260 s |
| Maintained mypy entrypoint | 0; passed | 45.078387 s |

The pytest controller recorded 193 passing setup/call/teardown sequences and a 28.711032-second pytest interval. The driver checked exact clean source before/after every gate and after cleanup. Tool pins matched Python 3.12.3, uv 0.11.24, pytest 9.1.1 and its locked plugins, Ruff 0.16.0, REUSE 6.2.0, and mypy 2.3.0. These were direct pinned-environment commands; `mise` was unavailable. Original logs, lane/controller receipts, driver bytes, and pin observations are in [evidence/verification](evidence/verification/).

Before freezing, 107 exploratory cases passed on the edited tree. An earlier attempt exited 2 during collection because pinned pytest rejects generator-valued parametrization under warnings-as-errors. Materializing the generator as a tuple corrected collection; no reader semantic edit was needed. Both original receipts/logs and their source-file hashes are retained. They are not presented as clean-source qualification.

## Execution, storage, and limits

The comparison's original child, maintained wrapper, and lane exited **0 without retry**, on the same clean proposal at both boundaries. Whole lane elapsed was **68.517607 seconds**, run ID `1d27a34456a44c2d841e2582c466188c`. Its actual metadata/profile scratch was removed and external TMPDIR was empty. The verification driver also exited **0**, preserved the exact source, and removed its owned temporary tree. Profile and quality gates ran sequentially.

| Original artifact | SHA-256 |
| --- | --- |
| Full comparison run record | `ae1984103663148a37e1b30f5f55faf49a708439811624df8a8c6b8003a38593` |
| Comparison execution receipt | `f3c5bcf7e03941a79d9a4e99c8844b6df293d7bda0c84be6a3df099dd43f998a` |
| Frozen comparison script | `9fa7b0517f505555e3758e1c024147e0c2ef28d1b72bf30c602af178dd275d0a` |
| Exact adversarial case document | `b7d78c0fd1338e1d5ddc985b90bc6bdb73fa02c0d76c865d21cfb19007879998` |
| Ordered receiving-input manifest | `130e6074038cdfc95b064d5467fc93f58545d93e7489a9e3382647fc93f48341` |
| Completed verification run record | `80316a0169bfa2284826180ea1574b16670262210a9dc82f3fd27dace23519cd` |

Large original inputs/receipts are losslessly compressed as gzip/base64 text. [VERIFY.md](VERIFY.md) restores and hashes the exact original bytes, including all per-edge/node inventories and the float-preserving adversarial document. [The compact profile summary](evidence/profile/summary.json) is derived navigation only; it does not replace the full original receipt. [SHA256SUMS](SHA256SUMS) covers the reference packet except itself. Compression and summaries create no evidence acceptance format or cache.

No global cache, accepted-result cache, checked-bound memo, contract change, weakened limit, new performance threshold, full census, physical publication campaign, or 1,000+ trial is part of this proposal. Numeric lookup reuse and source-context indexing were left outside the narrow change. Whole-path growth, product cost attribution, underutilized parallelism, full CI, compose completion, and PostgreSQL qualification remain outside this reference's evidence.
