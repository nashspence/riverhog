# Actual validation and integration route

## Tested source

All successful final checks use PR #1009 head `4d1bd1b7ead7c638e4a80137def7d406937098aa` plus the six-file patch. [SOURCES.json](SOURCES.json) records the six exact output hashes and patch identity. The affected existing production/reaper-test files match requested diagnostic source `8d278b6f7bc443a9919bcc5a6409a647cfd20f43` and main `40cf854adc346421b161a03e7a79a8cb5e0e3632` before this patch.

The patch was also actually applied in an isolated checkout of that main base. All six output files matched the tested outputs. This verifies application and source identity, not complete qualification of the main worktree or a later merge checkout.

## Results

| Check | Result |
| --- | --- |
| Three new maintained modules | 28 cases passed: 15 readiness/commit, 12 wake/lifecycle, 1 actual activation/receiving. |
| Selected existing reaper, terminal, due-selection, physical admission, claim-registration, retry and unrelated-work proofs | 40 cases passed. |
| Combined focused invocation | 68 passed in 82.38 s. |
| Existing complete readiness-scheduling growth witness | 1 passed in 20.85 s; 238 samples. |
| Pinned Ruff lint/format and focused mypy | Passed on the changed source; six changed files formatted. |
| `git diff --check` and patch application/output comparison | Passed. |
| Finite old/new queue-selection probe | Six observations completed; exact current source unchanged during the probe. |

Runtime: Linux x86_64, Python 3.12.3, SQLite 3.45.1. The existing locked workspace was installed using pinned uv 0.11.24 with `uv sync --locked --all-packages --group dev --python 3.12.3`; Ruff 0.16.0 and mypy 2.3.0 came from that environment. No dependency or lockfile changed. The small upload fixtures use their existing low-cost KDF/memory-port conditions, not the real-path PostgreSQL/filesystem/KDF-18 profile.

An initial direct growth-test invocation failed before contract evaluation because the qualification numeric-resource environment was absent. It was rerun successfully after loading the existing source-owned profile with `bind_numeric_environment(os.environ, qualification_numeric_policy())`. No resource value, growth bound, production budget or shared admission rule was changed. Earlier local test-fixture/type issues were corrected before the successful final runs; only final candidate identities support the passing claims.

## Focused commands

From the patched checkout, using the locked interpreter:

```bash
python -m pytest -q \
  tests/unit/test_archive_maintenance_readiness.py \
  tests/unit/test_archive_maintenance_wake.py \
  tests/unit/test_archive_maintenance_activation.py \
  tests/unit/test_archive_write_reaper.py \
  tests/unit/test_finalization_terminal_admission.py \
  tests/unit/test_finalization_due_selection.py \
  tests/unit/test_archive_upload_execution.py::test_published_future_is_not_physical_worker_join_or_free_capacity \
  tests/unit/test_archive_upload_execution.py::test_forward_exhaustion_preserves_target_isolation_and_terminal_headroom \
  tests/unit/test_archive_upload_execution.py::test_terminal_target_limit_leaves_independent_terminal_work_available \
  tests/unit/test_finalization_claim_registration.py::test_actual_claim_registration_resolves_rollback_and_lost_commit_reply \
  tests/unit/test_finalization_claim_registration.py::test_fresh_claim_waits_for_unacknowledged_original_workflow_receiver \
  tests/unit/test_archive_finalizer_resumption.py::test_original_resumption_obeys_the_existing_maintenance_cadence \
  tests/unit/test_archive_finalizer_resumption.py::test_maintenance_cannot_retire_a_healthy_result_before_actual_worker_exit \
  tests/unit/test_archive_upload_maintenance.py::test_terminal_backoff_and_capacity_leave_an_independent_construction_progressing
```

The growth witness requires the existing qualification-owned numeric evaluator identity:

```python
import os
import pytest
from scripts.validation_numeric import bind_numeric_environment, qualification_numeric_policy

bind_numeric_environment(os.environ, qualification_numeric_policy())
raise SystemExit(pytest.main([
    "-q",
    "tests/unit/test_upload_journal_service_coordination_growth.py::test_actual_service_readiness_scheduling_owns_complete_transaction",
]))
```

Lint/format cover the six changed files; mypy covers `riverhog/src/riverhog_api/app.py` and `riverhog/src/riverhog_core/services/collection_uploads.py`. Test results, selected measured properties and compressed growth samples are preserved with their identities in [evidence/validation.json](evidence/validation.json).

## Claim-to-proof map

| Claim | Maintained evidence |
| --- | --- |
| Notify after actual queue transition, commit and session close | New readiness visibility/close test; rollback/lost-reply tests; existing exact point-update races reused. |
| No extra SQL and no duplicate transition hint | Bound/unbound actual SQL comparison and repeated point call. |
| No lost wake around wait/sweep, bounded coalescing, safe lifecycle | Before/during wait, blocked sweep, delayed callback, 1,024-hint burst, failed provider switch/rebind and shutdown tests. |
| Actual first work activates after idle | New real service/reaper/physical-finalizer test with one-day fallback. |
| Hint cannot release a live original | Same test's held-exit thread plus existing Future/physical-receiving guards. |
| Due-time/current gate/claim and capacity remain independent | Existing terminal admission, bounded due selector, claim registration, retry cadence, terminal-headroom and unrelated-construction cases. |
| Existing scheduling growth contract is preserved | Existing complete transaction witness, 238 samples, unmodified numeric profile. |

## Integration order and limits

1. Reconcile the patch with current authorized integration source. The only production edits are the internal service callback and the API's local wake/selection/lifecycle handling. Three new test modules and a minimal fixture adaptation accompany them.
2. Run the focused checks above and the normal current-source PR checks. This reference does not substitute for the tested merge checkout or installed/platform qualification.
3. Repeat one matched real publication diagnostic under the existing conditions. Timestamp the last relevant receipt, successful point queue commit/session exit, notified/ordinary sweep, current claim/admission, first physical work, complete publication and actual final receiving. Preserve full operation output and authority checks.
4. Report residual activation, execution and receiving costs separately. Do not infer spare CPU, extrapolate the local 400 ms experiment to 30 s savings, or treat False-returning stages as dispensable.
5. Post the finite Proffer disposition against the actual published reference SHA and actual integration SHA, then continue the other work account.

Not run: full `make lint`, full repository unit/build/distribution/platform qualification, changed-source PR CI or its merge checkout, a full changed-source production publication/recovery timing, or shared budget/profile acceptance. No new large-run gate is proposed. The neutral-history request remains separate; PR #1009 qualification and the broader #1006/#991 work may continue independently. #1007 retains downstream acceptance.
