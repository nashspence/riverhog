# Validation and integration

## Evidence actually produced

The final proposal changes two production owners, one existing growth witness and one new maintained test file. The byte identities and source relationships are in [SOURCES.json](SOURCES.json); complete case identities, results and execution conditions are in [evidence/validation.json](evidence/validation.json).

| Check | Result and scope |
| --- | --- |
| New completed-preparation maintained cases | 30 passed on final production/test bytes; 61.60 seconds |
| Existing preparation, replay, Native, publication and custody regression files | 220 passed on final production bytes; 470.76 seconds |
| Existing service-unit growth file | 5 passed; 20 top-level execution samples and two import-order cases; 329.05 seconds |
| Complete actual-caller comparison | Six baseline/candidate pairs, 12 observations; identical exact journal bytes, default Native budgets and terminal anchors; final candidate byte hashes checked before/after |
| Unsafe cold-shortcut counterexamples | Two reproduced on a clean exact baseline; original restoration rejects changed epoch/topology metadata that binding plus cold certificate admission alone does not reject |
| Static/source checks | Pinned Ruff check/format, focused mypy for both production owners, whitespace check, and clean isolated patch application with exact resulting file hashes |
| Proffer handoff/publication | Validated against pinned Proffer; published inventory, product tree, parent and branch identity verified separately |

The growth run started before Ruff wrapped one caller expression. The earlier complete file was reconstructed and matched its recorded SHA-256; its full AST, including docstrings, equals the final source. Replay and growth-test files are byte-identical. This formatting-only relationship is recorded explicitly; the twelve comparison observations and new maintained cases use exact final production bytes.

The broader regression run started while the separate, unselected new test file was still being completed. Its two production owners remained byte-identical throughout, and all nine selected existing test files match the audited base. The new file was separately qualified after its final edits. A worktree-wide source-change flag therefore does not imply that the broader run used changed executable inputs; the validation record gives the exact before/after files.

All elapsed times above are ordinary validation observations. Instrumented probe timings overlapped other validation and are advisory. They are not product latency, a speedup estimate or an acceptance limit.

## Runtime and conditions

Execution used Linux x86_64, CPython 3.12.3, SQLite 3.45.1 and the unchanged locked Python dependencies from the existing research runtime. Ruff is 0.16.0 and mypy is 2.3.0. The unchanged qualification numeric policy was bound for maintained growth checks. No limit, fixture work budget, feature setting or Native advancement budget was widened.

Parent and child Python import paths selected the relevant worktree. The paired probe attests representative imported owners, eight production source hashes, the same Native extension binary and its unchanged defining build sources. The file-backed SQLite factory uses the production `autoflush=False`/`expire_on_commit=False` settings. All fixtures use their real Native implementation; no alternate executor or artificial turn acceleration appears in the complete paired probe. The maintained growth witness retains its separate existing fixture acceleration, which is explicitly distinct evidence.

This environment is not the separately approved GitHub Actions prepared-storage environment. No Actions approval, current profile admission or absolute operation-budget pass is claimed.

## First integration step

Read [DESIGN.md](DESIGN.md), reconcile the actual current authoritative main and owning-issue decisions, and apply the whole [prepared-continuation.patch](proposed/prepared-continuation.patch) in an isolated integration checkout. The two-owner implementation and its rejection/growth witnesses belong together. Do not merge the external reference branch.

Run the focused check under the repository's pinned environment:

```bash
mise x -- uv run --locked --all-packages --group dev python - <<'PY'
import os
import pytest
from scripts.validation_numeric import bind_numeric_environment, qualification_numeric_policy
bind_numeric_environment(os.environ, qualification_numeric_policy())
raise SystemExit(pytest.main([
    '-q', '-o', 'junit_family=legacy',
    'tests/unit/test_upload_journal_prepared_continuation.py',
    'tests/unit/test_journal_replay_binding.py',
    'tests/unit/test_upload_journal_service_unit_growth.py',
]))
PY
```

The broader existing files exercised here are:

- `tests/unit/test_journal_epoch_scan.py`
- `tests/unit/test_journal_topology_capture.py`
- `tests/unit/test_journal_epoch_union.py`
- `tests/unit/test_journal_epoch_rank.py`
- `tests/unit/test_journal_replay_binding.py`
- `tests/unit/test_journal_validation_handoff.py`
- `tests/unit/test_upload_journal_validation_owner.py`
- `tests/unit/test_upload_journal_validation_custody.py`
- `tests/unit/test_upload_journal_certificates.py`

Then use the repository's normal `make lint`, focused testing and exact-current-source PR qualification. Existing full integration, tested-merge and acceptance requirements are neither waived nor enlarged by this reference. The finite issue disposition should say what was adopted, independently reproduced, partially used or rejected, and what qualification remains.

## Reproducing the finite experiment

The packet includes the exact fixture bytes and raw observations, so new random fixture generation is unnecessary. Decompress `evidence/preparation-fixtures.json.gz` into a temporary fixture JSON. Use two isolated checkouts: clean audited baseline `60612eded1e0e4e5188c28a84697435eb969fad0`, and that baseline plus the complete patch. Provision their normal locked workspace/native build, then run `experiments/preparation_probe.py` with `--repo`, `--fixtures` and a distinct `--output` for each. It rejects an existing output path to preserve earlier observations.

Use `experiments/compare_preparation.py --baseline BASELINE.json --candidate CANDIDATE.json --output COMPARISON.json` to check fixture/source/native identities, equal Native turns and currentness/checkpoint counters, exact results, absent repeated preparation advancement, and original receiving.

`experiments/native_audit_probe.py --repo CLEAN_BASELINE --output AUDIT.json` reproduces the two metadata-admission counterexamples. It requires a clean baseline and checks imported source against Git. Its PostgreSQL query compilation is explicitly not live PostgreSQL testing.

## Failures and limits retained

Exploratory runs and unsuccessful fixture/setup attempts are preserved in `evidence/development-observations.json.gz`. Initial new-test failures used a graph-preparation fixture that was not a complete valid semantic journal, SQL arithmetic incompatible with the fixed-hex version encoding, or a NULL mutation rejected by a catalog CHECK before reaching the caller. They were replaced with valid semantic journals and representable adverse mutations; no production logic change was needed. An initial growth report used JUnit xunit2 with `record_property` and failed setup; the successful run uses xunit1. None is counted as passing final evidence.

Not executed or claimed here: full `make lint/unit/build/dist`, full current-source/tested-merge GitHub CI, installed/platform qualification, live PostgreSQL execution/concurrency/plans, a production-KDF full publication diagnostic, a whole-operation growth bound, per-file marginal cost, or shared approved Actions budget acceptance.

The logical SQL counters include catalog validation-unit execution events and separate transaction events. They exclude complete Native driver/filesystem primitives and final service-close primitives; actual final receiving is independently asserted. The correction case varies frame count and epochs together. These limits prevent turning useful finite evidence into a broader performance claim.
