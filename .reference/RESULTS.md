# Finite observation record

## Source identities

Baseline: clean issue-authorized source `8c80903818b386f50ddce8cd316cb3d93ec8cc51`, before any proposed edit. Initial compilation experiment: clean published `7c78eb33fd310653468c1a2fa9dfac41417c5adf`. Selected implementation: clean published **`af68d203f93765bddf9ad89a233a582266f10ce2`**, tree `3a031767fa41b4392855ee273d94d0fa5937c9d9`. The final handoff appends only `.reference` documents and evidence to that selected implementation; its package, product, support, and test blobs are unchanged. No dirty working-tree measurement is presented as clean-source qualification.

The selected proposal removes the experiment's package edit and uses only bounded observer ownership with the original public reader. The entire `packages/` and `riverhog/` trees match the audited base. The growth-reader file SHA-256 is `1adc9766c8d0c97cf97ce926695e6c38dc7d50458725e8947b57bfc94c90e376`.

## Selected bounded-helper result

The exact helper comparison ran both paths on clean `af68d203f93765bddf9ad89a233a582266f10ce2`, using three copies of one archived actual unit/caller pair. Each copy emitted the unit first and then the caller containing that newly emitted unit. Primitive metrics came from the actual retained observations, never from a bound formula. No fresh product operation was executed in this replay.

| Observed work | Immediate helper | `SampleBatch`, limit 4 |
| --- | ---: | ---: |
| Projection compilations | **12** | **5** |
| Public execution-reader calls | 6 | 4 |
| Roots in each public call | 1, 1, 1, 1, 1, 1 | 2, 2, 1, 1 |
| Distinct emitted roots independently received | **6** | **6** |
| Actual nested sample-node checks | **435** | **435** |
| Canonical equality with retained input | Passed | Passed |
| Uninstrumented wall seconds | 19.287943 | 12.564396 |
| Uninstrumented process CPU seconds | 19.256510 | 12.530675 |

The measured wall reduction was **34.86%** for this fixed replay. Counts were collected in an instrumented pass; all five monkeypatches were restored and checked before a separate single uninstrumented timing observation. The order was immediate first, then batched. Both modes prepared input copies outside timing. Timed batching includes entry, source admission, sample construction and sealing, source checks, automatic and final flushes, reference release, and receipt assertions. Single fixed-order observations do not establish statistical significance, full-census attribution, or product throughput improvement.

The instrumented receiver proved that every emitted root object was independently received exactly once. The unit has 71 nodes and the caller 74, so three pairs retain **435 actual recursive checks**. The batched scope reported six validated roots and four public reader calls, then closed with empty pending/source/root/digest collections and released projection bytes.

The original profile child and maintained lane both exited **0**, with clean source at start/end. The complete lane elapsed **68.320798 seconds**, including imports, both count passes, both timing observations, assertions, and scratch cleanup. Only profile scratch was created and removed; no new product fixture was used, and the external TMPDIR was empty after completion.

Exact successful script: [evidence/profile-batch-pair.py.txt](evidence/profile-batch-pair.py.txt), SHA-256 `f20fd77d592e5af2df137f7b13a3451846d854a453c54b899e845c3ec74413fb`. Original [run receipt](evidence/batch-pair-run.json), [lane receipt](evidence/batch-pair-lane.json), and [child output](evidence/batch-pair.log) are retained unchanged. The run receipt SHA-256 is `e36c3980687e436a27b83ffa7794272f01fecf88a856316289ad06a3050924f1`.

## Clean-source correctness and quality checks

All checks below ran on clean `af68d203f93765bddf9ad89a233a582266f10ce2`, after profiling ended. The verification driver and every maintained lane returned 0. Exact clean source matched at start and end, and the owned external pytest temporary tree was removed after original child completion.

| Check | Actual result | Maintained lane elapsed s |
| --- | --- | ---: |
| Four focused test modules | **118 passed in 95.35 s** | 98.092975 |
| Repository-wide Ruff | Passed | 0.171612 |
| Repository-wide format check | 2,080 files already formatted | 0.098312 |
| REUSE | Passed; 2,417/2,417 covered files | 1.838251 |
| Strict repository mypy entry point | Passed | 4.326937 |

The focused modules were `test_growth_observer_batch.py`, `test_growth_declarations.py`, `test_validation_witnesses.py`, and `test_private_growth_discovery.py`. They cover 34 new batch cases, including one actual three-turn caller fixture, and existing standalone declaration/serialized-evidence/source-owner regressions. The actual fixture checks all six caller/unit roots, independent canonical equality through the immediate helper, explicit generator closure, and zero checked-out catalog connections before and after closure. Invalid samples, later failures, orphaned units, caught admission errors, stale bindings, same-ID callable substitution, mutated metadata/samples, malformed mappings, and closed/reentered scopes are exercised.

Existing reader regressions cover units, numerical rejection of asymptotic bounds, exact delegate multiplicities/mappings, complete sample fields/digests, cyclic edges, duplicate ownership, and the 65,536-sample-node / 131,072-expression-node budgets. They are metadata correctness tests, not a 1,000+ product publication trial. Graph-depth and delegate relation-strength validation also remain byte-for-byte unchanged in the public reader; no new compilation shortcut is delivered.

Original commands, exits, source identities, all lane logs, and the machine-readable pytest record are in [evidence/verification/](evidence/verification/). [The driver record](evidence/verify-driver.py.txt) preserves the actual producer orchestration. These are focused producer receipts, not full integration qualification.

The successful baseline profile script is preserved byte-for-byte in [evidence/profile-script.py.txt](evidence/profile-script.py.txt). The initial compilation experiment used that exact script on the exact retained projection/sample. It is an experiment record, not a new maintained profiling entry point. It runs under the existing `scripts/ci_timing.py run` recorder. [VERIFY.md](VERIFY.md) records reproduction and receiving commands.

## Baseline with actual observed input

One small actual service caller operation supplied the retained sample: 4,913 source octets, 4,913 observed framing octets, two stamp calls, one unit call, one Native-owner call, and 74 nested execution nodes with depth 8. The fixture used `text_octets=0` and `unrelated=0`; no direct Native fast-forward or wide publication campaign occurred.

| Fixed phase | Expression-unit entries | Uninstrumented CPU s | Uninstrumented wall s |
| --- | ---: | ---: | ---: |
| One `project_bindings` | 68,785 | 1.501703 | 1.503943 |
| One `validate_projection` | 68,785 | 1.524197 | 1.528678 |
| Three serial validations of the same actual sample | 206,355 | 5.467304 | 5.480917 |
| One three-entry batch repeating that same sample | 68,785 | 2.542783 | 2.545670 |

Each phase used one instrumented count pass and a separate single uninstrumented timing observation. There is no timing target, pass threshold, confidence interval, or cross-host generalization. The last two rows reuse one observed tree, not three distinct product executions.

The complete baseline child and lane wrapper both exited 0; the maintained lane elapsed 56.778242 seconds, including imports, fixture execution, assertions, retained inputs, measurements, and cleanup. The generator closed, catalog checkout count was zero, and the private fixture directory was removed. That lane total is a receipt, not comparable with a replay-only proposal lane.

## Compilation experiment excluded from the delivered source

The initial candidate used a compilation-local set of successfully checked declaration/metric bodies. Its clean source is `7c78eb33fd310653468c1a2fa9dfac41417c5adf`, tree `aeb9e40cd580571859647836a293ee8493e41188`, directly above the audited base. The declaration projection and actual sample remained canonical-byte identical to the baseline.

| Fixed phase | Baseline entries | Experiment entries | Baseline CPU / wall s | Experiment CPU / wall s |
| --- | ---: | ---: | ---: | ---: |
| `project_bindings` | 68,785 | 47,743 | 1.501703 / 1.503943 | 1.358551 / 1.358854 |
| `validate_projection` | 68,785 | 47,743 | 1.524197 / 1.528678 | 0.971173 / 0.971383 |
| Three serial validations of the same retained sample | 206,355 | 143,229 | 5.467304 / 5.480917 | 6.741306 / 6.772027 |
| One three-entry batch repeating that sample | 68,785 | 47,743 | 2.542783 / 2.545670 | 2.978158 / 2.989522 |

The 30.59% traversal reduction did **not** establish an overall elapsed improvement: the sample-validation wall observations were 23.56% and 17.44% slower. A single observation does not establish a universal regression either. The producer therefore excluded the memo from the delivered proposal instead of promoting counts alone into a performance claim. Its source/tests remain in the append-only historical commit, and its original receipts are retained as `evidence/compilation-experiment-*`.

Original child and lane exited 0, clean source identity matched at start/end, and the external TMPDIR was empty. The lane elapsed 32.295643 seconds, including replay setup and all four measurement passes. No fresh fixture was created: the script's `fixture_created=false, fixture_removed=true` pair is its replay marker, not a fresh product cleanup receipt. The successful regeneration/standalone-validation log is also retained.

## Retained identities and failure record

| Artifact | SHA-256 |
| --- | --- |
| Successful profile script | `f484d2458814c74af03d24d93a3bad24c54f25f4dbdac1f47d966f4065b79c3f` |
| Canonical 100-declaration projection (4,512,672 bytes) | `f8eae73e293eac5e9ed7dcf01d706d9176d4f198e13ef61203d06d0df41c0ef1` |
| Actual retained sample (225,374 bytes) | `bb2a6cc9702d15c805fdb32d84ed14ea864162a31a623a7f412fcb011ff9f37f` |

The sample and original run/lane receipts are retained under `evidence/`. The redundant full projection is reproducible from exact source; receiving must verify its canonical hash before using it as a replay input.

The first baseline preflight exited 1 before fixture creation because the producer's version assertion initially rejected uv's platform suffix. That failed receipt and output are retained separately. The successful script checks the first two version tokens and otherwise uses the same protocol. The failed attempt supplied no profile measurements and is not counted as success.

## Exploratory validation and limitations

Before freezing the initial experiment, 52 new compilation tests and 33 new small batch tests passed. One actual three-turn caller test passed in 86.64 seconds: six caller/unit roots were received in four public batch calls, all six matched the immediate helper's canonical output, the fixture generator closed, and catalog checkout count was zero. These exploratory runs were on the edited working tree with the experimental memo. They do not qualify the final narrower source. Their temporary test directories were removed after their original children exited. The compilation-specific test module was removed with the excluded package edit; the batch test module now owns its small fixtures directly.

The selected implementation's clean-source verification is recorded above. The reference did not run full repository unit shards, distribution/image/release qualification, the complete caller census, or 1,000+ product executions. It establishes no explanation for all of the earlier 470.50-second interrupted census. `mise` is unavailable; the direct pinned-environment Ruff/format/REUSE/mypy commands were run, and the `make lint` frontend was not. The reference branch does not trigger the repository's main/release push CI; no PR or manual full qualification was requested or represented as passing.
