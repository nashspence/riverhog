# Reproduce the audited source, not the reference commit

These commands are **not executed results**. They target Linux/Bash with Git, Make, `mise`, and the repository's locked toolchain. Run from an existing trusted Riverhog clone with exclusive use of the new worktree. They do not dispatch a workflow, qualify providers, build a release, publish anything, or modify #866. Do not execute an entire release workflow to obtain these observations.

## 1. Dedicated source and external output

```bash
set -euo pipefail
SHA=18c5f287e7acb81ceaa84f5a3bf8deadbc3852e8
ROOT=$(git rev-parse --show-toplevel)
git -C "$ROOT" fetch origin "$SHA"
git -C "$ROOT" worktree add --detach "${ROOT}-873-audit" "$SHA"
cd "${ROOT}-873-audit"
test "$(git rev-parse HEAD)" = "$SHA"
test -z "$(git status --porcelain=v1 --untracked-files=all)"
OUT=$(mktemp -d /tmp/riverhog-873.XXXXXX)
export SHA OUT
printf 'source_sha=%s\noutput=%s\n' "$SHA" "$OUT" | tee "$OUT/identity.txt"
```

A pre-existing worktree path fails rather than being overwritten. Keep the new directory exclusively owned for the run. Do not copy `.reference/` into this checkout: that changes its source state. When integrating changed code, use its newly committed SHA and deliberately refresh this audit rather than relabeling the old evidence.

## 2. Extract only the two relevant commands from the pinned workflow

```bash
mise x -- uv run --locked --all-packages --group dev python - <<'PY'
import os
from pathlib import Path
import yaml
workflow = yaml.safe_load(Path('.github/workflows/release-qualification.yml').read_text())
steps = workflow['jobs']['release-audit']['steps']
for name, filename in (
    ('Exercise disposable operation lifecycles and record timings', 'selected.sh'),
    ('Verify exact-SHA operation evidence', 'predicate.sh'),
):
    matches = [step['run'] for step in steps if step.get('name') == name]
    assert len(matches) == 1, name
    Path(os.environ['OUT'], filename).write_text(matches[0] + '\n')
PY
export RIVERHOG_OPERATION_SOURCE_SHA="$SHA"
export RIVERHOG_OPERATION_TIMINGS="$OUT/operation-timings.json"
export PYTEST_ADDOPTS="-vv -ra --junitxml=$OUT/selected.xml"
bash --noprofile --norc -e -o pipefail "$OUT/selected.sh" 2>&1 | tee "$OUT/selected.log"
unset RIVERHOG_OPERATION_SOURCE_SHA RIVERHOG_OPERATION_TIMINGS PYTEST_ADDOPTS
make operation-qualification \
  args="evidence --source-sha $SHA --timings $OUT/operation-timings.json --output $OUT/operations.json" \
  2>&1 | tee "$OUT/producer.log"
test "$(git rev-parse HEAD)" = "$SHA"
test -z "$(git status --porcelain=v1 --untracked-files=all)"
```

`pipefail` stops on a failed selected run; preserve its log/timings as a counterexample and do not continue to the producer. The producer independently rejects nonzero pytest status and wrong/dirty source boundaries. Confirm actual observed/required counts from this generated report; do not compare to old issue-comment totals as fixed budgets.

## 3. Check honest statuses and expected consumer refusal

```bash
mise x -- uv run --locked --all-packages --group dev python - <<'PY'
import json, os
from pathlib import Path
payload = json.loads(Path(os.environ['OUT'], 'operations.json').read_text())
assert payload['source_sha'] == os.environ['SHA']
claims = payload['qualification']
for key in ('positive_local_lifecycles', 'cli_human_json_projection',
            'bounded_state_access', 'event_cursor_restart_resume'):
    assert claims[key]['status'] == 'not_established', key
local = claims['event_cursor_restart_resume']['local_api_process_restart']
assert local['status'] == 'passed'
expected = {('riverhog', 'list_lifecycle_events'), ('stove0', 'list_events'),
            ('a-riverhog-ftp-spool', 'list_ftp_spool_events')}
assert {(row['application'], row['operation_id']) for row in local['operations']} == expected
assert len(local['operations']) == len(expected)
for row in local['operations']:
    assert row['status'] == 'passed'
    assert row['test_nodeid'] == (
        'tests/unit/test_event_cursor_restart.py::'
        'test_event_cursor_continues_across_process_restart[' + row['application'] + ']'
    )
    assert '/blob/' + os.environ['SHA'] + '/' in row['assertion_source']
assert claims['positive_local_lifecycles']['operations_with_successful_responses'] == len(
    payload['performance']['local_api']['operations'])
print('Narrow local evidence present; all four release-level claims remain unestablished.')
PY
set +e
SOURCE_SHA="$SHA" OPERATIONS_SUMMARY="$OUT/operations.json" \
  bash --noprofile --norc -e -o pipefail "$OUT/predicate.sh" >"$OUT/predicate.log" 2>&1
PREDICATE_STATUS=$?
set -e
printf '%s\n' "$PREDICATE_STATUS" >"$OUT/predicate.exit"
test "$PREDICATE_STATUS" -eq 1
grep -qx false "$OUT/predicate.log"
```

A missing tool, malformed input, or different failure is not an acceptable replacement for the expected `false` predicate result. Never patch statuses to make this step accept. Any unexpected success is a rejection-control defect to investigate, not a qualified release.

## 4. Isolated adversarial regressions

Run separately from the observation-producing session; these tests deliberately create synthetic evidence, failed child runs, and dirty temporary repositories.

```bash
env -u RIVERHOG_OPERATION_SOURCE_SHA -u RIVERHOG_OPERATION_TIMINGS -u PYTEST_ADDOPTS \
  make unit UNIT_PYTEST_ARGS= \
  TESTS="tests/unit/test_operation_qualification.py tests/unit/test_qualification_source.py" \
  args="-vv -ra --junitxml=$OUT/regressions.xml" 2>&1 | tee "$OUT/regressions.log"
test "$(git rev-parse HEAD)" = "$SHA"
test -z "$(git status --porcelain=v1 --untracked-files=all)"
sha256sum "$OUT"/operation-timings.json "$OUT"/operations.json \
  "$OUT"/operations.json.md "$OUT"/selected.xml "$OUT"/regressions.xml \
  "$OUT"/selected.log "$OUT"/producer.log "$OUT"/predicate.log \
  "$OUT"/predicate.exit "$OUT"/regressions.log >"$OUT/artifact-sha256.txt"
```

Retain the identity, exact commands, logs, JUnit results, timing/report files, predicate exit/output, and hashes with the owning issue. Record source start/finish state and environment/tool versions. Separate the expected failed child cases from the enclosing successful regressions. For integration, execute the repository's normal gates on the eventual integration commit as well; this reproduction recipe is not a substitute for them. No result at the audited base automatically qualifies the external reference SHA or any later source.
