# Finite observation record

## Source identities

Baseline: clean issue-authorized source `8c80903818b386f50ddce8cd316cb3d93ec8cc51`, before any proposed edit. Initial compilation experiment: clean published `7c78eb33fd310653468c1a2fa9dfac41417c5adf`. The selected proposal removes that experiment's package edit and uses only bounded observer ownership with the original public reader. Its exact clean source identity and direct helper measurements will be appended after the narrower implementation is frozen. No dirty working-tree measurement is presented as clean-source qualification.

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

The clean-source verification result belongs in the subsequent appended record. The reference does not run full repository unit shards, distribution/image/release qualification, the complete caller census, or 1,000+ product executions. The 65,536-node and 131,072-expression existing reader-budget tests are metadata correctness tests, not a publication campaign. `mise` is unavailable; direct pinned-environment equivalents must be named rather than claiming that the `make lint` frontend ran.
