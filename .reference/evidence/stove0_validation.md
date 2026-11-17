# Stove0 focused evidence

## Scope and conditions

Source: `c28eceaa2dfec1b3712135469ba12230e74f5ffa`, from the isolated `riverhog-998` worktree. Product files were not edited. The reference imports `stove0_target_support`, `stove0_target_protocol` and `stove0_extension_support` from that worktree's actual source roots; their resolved paths are recorded in [stove0_progress.json](stove0_progress.json).

Environment: isolated `application-998-env`, Python 3.12.3, pytest 9.1.1, Ruff 0.16.0, Linux 6.18.57 x86_64/glibc2.41. The parent provisioned the repository's frozen workspace environment. This work did not install/change dependencies or previous task environments. `TMPDIR` and pytest basetemp point outside the worktree at the task-owned `998-stove0-temp`. Each pytest invocation binds the repository's `qualification_numeric_policy` and loads `scripts.ci_timing`.

The reference request and successful result are the maintained target-support test builders. The paired progress comparison starts at the already-admitted original acceptance/running boundary, then uses real execution-session callbacks and the actual target file writer. The actual-dispatcher cases replace only live Core authorization with the maintained synthetic-claim fixture convention. There is no real Core claim, archive publication, codec/GPU job, remote rclone write or live provider qualification. The supervised process witness is a local Python child that prints readiness, waits, is canceled and is actually received before the original lock is released.

The tests and count comparison qualify the advisory overlay only. They do not implement the broader private planner, snapshot generation, public hard cut, original-retention/capacity adapter or sealed-effect Core guard described in [STOVE0.md](../STOVE0.md).

## Observed results and correction history

| Run | Outcome | Artifact |
|---|---|---|
| First reference run | 11 passed, 2 failed, 2.09s | [JUnit](stove0_tests_first.xml), [timing](stove0_timing_first.json) |
| Corrected reference run | 13 passed, 1.82s | [JUnit](stove0_tests.xml), [timing](stove0_timing_final.json) |
| Ten selected maintained cases | 9 passed, 1 failed, 7.68s | [JUnit](stove0_maintained_tests.xml), [timing](stove0_timing_maintained.json) |
| Exact failed maintained case, environment correction | 1 passed, 1.23s | [JUnit](stove0_effect_corrected.xml), [timing](stove0_timing_effect_corrected.json) |
| Actual-owner 64-callback pair | Identical final original statuses; 64 advisory writes/128 `os.fsync` calls/23,927 encoded bytes became zero | [Measurement](stove0_progress.json) |

The first reference failures were test-construction defects. `TargetProgress` is already frozen: the corrected test asserts public assignment rejects and then deliberately bypasses that guard to verify the owner's independent copy. The capacity test initially resealed the request at its existing fence 2, producing the same job ID; the correction uses fence 3 and explicitly asserts a distinct job. No product code changed.

The maintained effect-receipt test initially constructed a real `httpx` client under the workspace's ambient SOCKS proxy variables. That constructor failed because the frozen environment does not include optional `socksio`; the retained status reported that exact ImportError before the fixture effect was invoked. The single rerun omitted proxy variables for this synthetic no-network test, without adding a package or changing source, and passed. The other nine selected cases already passed and were not repeated. This is 23 distinct focused cases observed passing after the stated corrections, not an initially uninterrupted green campaign.

The paired sample's callback-only times were 53,698,308 ns current and 264,129 ns proposed. These times are one ordered local sample, not a reproducible performance threshold or an application throughput claim. Acceptance plus running still made 2 file selections and 4 sync calls, and final original success still made 1 selection and 2 sync calls in each mode. Four point status reads made no writes in either mode. Counts observe Python writer and `os.fsync` calls; they do not measure physical device IO, cache effects, a power-loss experiment or another platform's durability.

After measurement, one comment was corrected from “five” to “four” advisory fields. The measurement JSON preserves the measured source hashes and records this comment-only correction separately. No executable experiment behavior changed after the passing run or measurement.

## Reproduction

Run from the `riverhog-998` worktree using its isolated Python. Choose a fresh task-owned basetemp/result directory for each run. The following is the exact shape used for the corrected reference run:

```bash
/workspace/scratch/939b84c043b0/application-998-env/bin/python - <<'PY'
import os
from pathlib import Path
root = Path('/workspace/scratch/939b84c043b0/998-stove0-temp')
os.environ['TMPDIR'] = str(root)
os.environ['RIVERHOG_CI_TIMING_DIR'] = str(root / 'timing-final')
os.environ['RIVERHOG_CI_LANE'] = '998-stove0-reference'
from scripts.validation_numeric import bind_numeric_environment, qualification_numeric_policy
bind_numeric_environment(os.environ, qualification_numeric_policy())
import pytest
raise SystemExit(pytest.main([
    '-q', '-p', 'scripts.ci_timing',
    '.reference/implementation/test_stove0_progress_experiment.py',
    '--basetemp=' + str(root / 'pytest-final'),
    '--junitxml=.reference/evidence/stove0_tests.xml',
]))
PY
```

The maintained run used the same wrapper with lane `998-stove0-maintained`, `timing-maintained`, `pytest-maintained` and these exact node IDs:

```text
some-implementations/stove0/packages/target-support/tests/test_target_support.py::test_persisted_effect_receipt_replays_without_repeating_external_effect
some-implementations/stove0/packages/target-support/tests/test_target_support.py::test_uncertain_effect_commit_stays_interrupted_and_never_auto_repeats
some-implementations/stove0/packages/target-support/tests/test_target_support.py::test_published_success_wins_late_cancel_and_cleanup_failure
some-implementations/stove0/packages/target-support/tests/test_target_support.py::test_checkpoint_retention_keeps_failed_unpublished_evidence
some-implementations/stove0/packages/target-support/tests/test_target_support.py::test_output_checkpoint_rejects_replacement_and_changed_workspace
some-implementations/stove0/packages/observer-support/tests/test_observer_lifecycle.py::test_canceling_payload_waits_for_actual_consumer_stop
some-implementations/stove0/packages/observer-support/tests/test_observer_lifecycle.py::test_valid_complete_evidence_wins_late_cancel_and_cleanup_failure
some-implementations/stove0/application/tests/test_work_state.py::test_target_callback_authority_rejects_unpermitted_disposition_and_stale_fence
some-implementations/stove0/application/tests/test_work_state.py::test_target_production_seal_is_segmented_closes_declarations_and_replays
some-implementations/stove0/application/tests/test_work_state.py::test_stale_revision_and_invalid_success_order_fail_closed
```

For the corrected effect-only invocation, before creating any clients the wrapper removed `ALL_PROXY`, `HTTPS_PROXY`, `HTTP_PROXY` and their lowercase forms from that process's environment. It used only the first node above, lane `998-stove0-maintained-effect`, directories `timing-effect-corrected`/`pytest-effect-corrected`, and `stove0_effect_corrected.xml`. Proxy values were not logged.

The count sample imports `run_pair` from `.reference/implementation/stove0_progress_experiment.py` after numeric policy binding and calls `run_pair(fresh_external_temp_path, callbacks=64)`. The CLI runs the same function:

```bash
TMPDIR=/workspace/scratch/939b84c043b0/998-stove0-temp \
/workspace/scratch/939b84c043b0/application-998-env/bin/python \
  .reference/implementation/stove0_progress_experiment.py \
  /workspace/scratch/939b84c043b0/998-stove0-temp/paired-new-run --callbacks 64
```

The measured runner added source commit, environment paths/versions and file hashes to the returned JSON; it did not change the experiment. Its supplied root must not already exist, preventing accidental replacement of a prior sample.

Formatting/lint used the pinned executable with `ruff format --check` and `ruff check` for both Stove0 reference Python files. All original services and child processes in the added tests are received and closed in `finally` paths. The failed maintained fixture's original worker had already returned its recorded software failure; the pytest process exited normally with a failing test status. No live external tool ownership was discarded as part of the environment correction.
