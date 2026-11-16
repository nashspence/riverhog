# Actual reference validation

Date: November 16, 2026. Final audited/tested product source: **c28eceaa2dfec1b3712135469ba12230e74f5ffa**.
Only `.reference/` differs from that base. These results qualify the stated reference experiments; they do not establish integrated Native/Core behavior or issue completion.

## Executed checks

**72 tests passed, zero skips, failures or errors:** 69 reference cases and three unchanged Native baseline cases.

| Check | Result | Observed command elapsed seconds |
| --- | --- | ---: |
| focused-tests | PASS | 42.501913 |
| ruff-lint | PASS | 0.039453 |
| ruff-format | PASS | 0.038385 |
| strict-mypy-storage | PASS | 0.278863 |
| repository-lint | BLOCKED: missing mise | 0.013721 |
| product-source-identity | PASS | 0.011841 |

Reference cases comprise 22 real-Native semantic/failure/lifetime cases, 20 immutable generation-store cases, 19 WAL-capture cases and eight combined capture/generation/separate-selector cases. The three baseline cases are `test_public_session_matches_native_summary_and_exact_source` at its existing row-budget parameterizations. Full case names/results/times are in [validation/pytest.xml](validation/pytest.xml); commands, output, tool versions, exit statuses and implementation hashes are in [VALIDATION.json](VALIDATION.json).

The final focused pytest command was:

```bash
TMPDIR=/workspace/scratch/939b84c043b0/996-temp \
/workspace/scratch/939b84c043b0/native-996-env/bin/python -m pytest -q \
  .reference/implementation \
  packages/riverhog-provenance/tests/test_journal_session.py::test_public_session_matches_native_summary_and_exact_source \
  --basetemp=/workspace/scratch/939b84c043b0/996-final-pytest \
  --junitxml=.reference/validation/pytest.xml
```

Run from the reference checkout. Equivalent local tool/temporary paths may be substituted; preserve pinned versions and source identity. Ruff covers all eight Python files. Strict mypy covers `generation_store.py` and `wal_capture.py`, not the instrumented Native experiment or the repository. Independent checks ran concurrently; their small elapsed observations are not isolated product measurements.

## Pinned environment

Python **3.12.3**, uv **0.11.24**, pytest **9.1.1**, Ruff **0.16.0**, mypy **2.3.0**. The actual linked SQLite library reports **3.45.1**. Native dependencies were installed from the unchanged `uv.lock` using pinned uv with `--locked --package riverhog-provenance --extra test --no-default-groups`; 29 dependencies and the retained-resource native extension were installed/built for the focused environment. This is not a complete workspace installation. Lock SHA-256: `16b64145aa11bfbd6d2076a7a5adc783ed7d2e000237f9c31e9977ef48815f24`.

[QUALIFICATION.md](QUALIFICATION.md) gives the reproducible installation command, maintained F1 route and full production gates. Physical backend support cannot be inferred from the Python version or a successful package import.

## Real Native observations

The experiment drives the actual maintained `JournalValidationSession`/`EntryReplay` semantic algorithms, replacing only the parent ledger/progress implementation in a reference subclass. It removes the `journal_validation` parent table rather than merely postponing its commit. Child checkpoints, per-unit child transactions, repeated prefix checks and `EntryReplay._start` encoding remain.

Three generated journals were captured once, then the **identical recorded bytes** were used for both arms. [NATIVE_EXPERIMENT.json](NATIVE_EXPERIMENT.json) retains those bytes/hashes, complete compared result evidence, separate counters/timings and source hashes. The 22-test suite also compares all five checked-in formal examples against baseline and their expected materialized JSON/schema. Negative cases preserve real recorder/sequence/archival-prefix errors and semantic-failure precedence; commit-failure and independent last-reader lifetime are tested.

| Captured input | SQL statements: baseline → experiment | Parent SQL | Total transaction commits | Parent encoded bytes | Observed elapsed seconds |
| --- | ---: | ---: | ---: | ---: | ---: |
| small (4,913 bytes) | 2,155 → 1,520 | 422 → 0 | 109 → 109 | 416,384 → 83,167 | 0.397787 → 0.212911 |
| correction (6,300 bytes) | 5,331 → 3,589 | 975 → 0 | 247 → 247 | 945,658 → 189,093 | 0.844420 → 0.450370 |
| journal_checkpoint (7,315 bytes) | 5,661 → 3,788 | 1,040 → 0 | 263 → 263 | 993,192 → 198,671 | 0.848521 → 0.432532 |

**The total transaction counts do not fall in this experiment.** The narrower result establishes that parent persistence/representation can be separated from real semantic acceptance while the old child machinery remains. All three pairs compare full materialization, documentary origins, frame preimages, journal identity/hash/extent and independently received results.

Each elapsed pair is one serial instrumented observation, baseline first, on a shared host without a controlled cache/load experiment or repeated randomized ordering. It is not a production speedup estimate, F1 result, default-policy tax measurement or recovery-benefit proof. Physical synchronization is explicitly `null`: Python SQLite tracing does not count SQLite VFS sync calls. Costs for capturing the input journals occur before both measured validation arms.

To reproduce the observations with the same captured evidence:

```bash
TMPDIR=/absolute/writable/temp \
/path/to/pinned-native-python .reference/implementation/reproduce_native_experiment.py \
  --captured .reference/NATIVE_EXPERIMENT.json \
  --output /absolute/output/native-observations.json
```

The reproducer checks that it imports this checkout and that product files match the audited base. It verifies captured hashes and complete expected evidence before emitting results. It is a reference-only experiment command; Riverhog's maintained profiling entrypoint remains `make profile`.

## Storage evidence

The storage tests use actual SQLite databases and files. They verify complete initial restoration; changed-page updates with at most eight copied map nodes per page; prior-root isolation through later mutations; shrink/regrowth; wrong bindings and missing/corrupt exact dependencies; bounded object collection and retained pins; and a real child process exiting after partial candidate staging.

WAL tests verify actual committed frames, both checksum byte orders, changed data/index/schema pages, truncation/regrowth, malformed/partial/stale/uncommitted tails, bounded index/WAL refusal, original file/owner changes, existing-spool preservation, and mandatory stream exhaustion. The reserved lock-page boundary and an omitted ordinary new page use bounded synthetic format witnesses, not a physical one-gigabyte database test.

The eight composed cases connect actual WAL extraction to the authenticated page store and a **separate test selector database**. They check independent reconstructed SQL results, no full main-DB read on later capture, previous selection before publication, stale epoch refusal, lost selection reply with exact reconciliation/no duplicate selected count, rejected-candidate retry and refusal of partially consumed or changed capture before envelope sealing.

These are mechanism/protocol tests. The selector is not Core's production authorization/accounting transaction; pin tokens are not retained-resource original receiving; direct paths/connections are not production admission. Process exit is not OS-crash or power-loss qualification. No integrated Native session resumes from this backend in the delivered branch.

## Source reconciliation and final identity

Initial research/experiments used `0f8030b0c0cd53eb33456924f070ae5979ed307e`. The live #979 head advanced to `c28eceaa2dfec1b3712135469ba12230e74f5ffa` before publication. The complete one-commit delta changes only `.gitattributes`, `tests/unit/test_validation_numeric.py` and `tests/unit/test_validation_witnesses.py`: Python LF checkout policy and its exact-byte regression coverage. Native/Core implementation, package tests, README/AGENTS, tool versions and lockfile are identical. The reference was fast-forwarded to that base and the final 72 tests and paired observations rerun on it. No historical #979 CI result is claimed for this reference.

The published commit/tree, audited parent and remote branch identity are verified and recorded in the owning #996 comment. There is no self-referential claim that this document contains its own final Git commit SHA. All product files must match the audited base; only `.reference/` may be present in the reference delta.

## Tested implementation hashes

| File | SHA-256 |
| --- | --- |
| `generation_store.py` | `28956d5215150c35e5f094515109deba3c9179cdbabb61efd2bd5207e0e6c9f8` |
| `native_execution_experiment.py` | `1493aac691bb8e241e2b761d33ad842e0398a49a33c7e9c20d37fe0595618bff` |
| `reproduce_native_experiment.py` | `a1a1fabe06637ccea78d276cdb487a906bb1206c3f7b86daed912a08941faeda` |
| `test_generation_capture.py` | `e64d91df6595931bad374d74ca03de48e4ef67f7566079f2a56f4e7b468f569d` |
| `test_generation_store.py` | `955a685ed9aa03a84aba5ca25082a1bbd8ff924f3c76841b6aaff12f9dda5337` |
| `test_native_execution_experiment.py` | `d7bac0e0085d94b4ee5088cc584686a30d85ef84dea4bbb6388f6c85420c04c1` |
| `test_wal_capture.py` | `509f95fce0e78862a4d2ec50c2e6f20810e4f089b7b9878b0079110528ca7729` |
| `wal_capture.py` | `681110f0befcc2fb15c6c2f9c23b95a7daadc91fc609b08e032c2750043ad573` |

## Blocked and unperformed checks

`make lint` was attempted on the final base and **did not start** because `mise` is unavailable on PATH. Make returned 2 after its wrapper error 127. Standalone pinned Ruff/mypy passes do not replace that required repository gate.

No complete repository CI or tested merge-checkout qualification is claimed. F1 authenticated/encrypted publication, integrated #995 grants and #997 adoption, actual long/unknown-length default-policy promotion, Native-generation restart, full lifecycle growth/receiving, installed independent recovery, PostgreSQL, macOS, Windows, physical synchronization or power-loss qualification were not performed. No provider operation, large held campaign, merge or release was authorized or run.

The exact reference SHA's workflow/check availability is reported on #996 after publication. A missing/skipped/blocked required witness is not a pass. [QUALIFICATION.md](QUALIFICATION.md) provides the concrete integration sequence and acceptance/fault/growth matrix, including adverse and unmatched measurement dispositions.
