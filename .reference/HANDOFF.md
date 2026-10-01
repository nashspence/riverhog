# NON-AUTHORITATIVE EXTERNAL REFERENCE

Issue: #515
Convention: #903
Reference branch: `reference/external/515-gogurt-deterministic-ci`
Audited base: `main` @ `99179cbb94a96d8ea2e633ab60c9e34be1763527`

Produced by: OpenAI ChatGPT
Model: GPT-6 Astra Pro
Configuration: No additional mode or effort configuration is exposed.
Requested by: Maintainer nashspence, for the Riverhog integration agent.
Published by: OpenAI ChatGPT through the connected GitHub interface; GitHub
Actions mechanically materialized the checksum-bound implementation as described
below. Commit account metadata is publication metadata, not producer identity.

## Purpose and authority

Provide concrete, researched implementation and regression-test input for #515:
correct Windows process settlement observations, deterministic test handoffs,
bounded failure evidence, and coverage-preserving CI performance groundwork.
This is not a claim that every historical Gogurt failure has been explained or
that CI has reached an empirically optimal wall time.

This branch is externally produced reference material. Its creation, testing,
publication, issue linkage, or request by an authorized repository agent does
not itself establish or extend an accepted design, contract, release requirement,
or authorization to integrate these changes. Authority remains with #515,
subsequent maintainer decisions, and the repository's normal integration rail.

The exact reference commit SHA recorded in the owning issue is the handoff
identity; the branch name is navigation only. Reconcile against then-current
authoritative repository state and owning-issue decisions, not mechanically.
Neither `main` nor `release/v1` was moved. The audited AGENTS.md freeze-candidate
rail remains authoritative; this reference does not select a freeze candidate,
close #515, approve integration, or qualify a release.

Publication interface limitation: the connected GitHub interface does not expose
the native issue-to-branch Development mutation. This must also be recorded in
#515. A Markdown link is not represented as a native Development relationship;
a capable publisher may establish that relationship without changing provenance.

## Contents and rationale

### Windows runtime correctness and targeted performance change

`some-implementations/gogurt/listener-host/windows/src/a_gogurt_windows_listener/__init__.py`
uses explicit pointer-width Windows HANDLE declarations and last-error-aware
calls. A zero-time wait distinguishes signaled/exited from timeout/running;
WAIT_FAILED and unexpected results are errors, not proof that the process died.
Positive nonexistent PIDs and access-denied observations are classified separately,
and invalid PID inputs cannot silently truncate into a different DWORD PID.
Handles are closed on every opened-handle path, with close failures surfaced.

The staged qualification harness independently enforces the same observation
semantics. It does not import a provider's implementation as its own oracle.
Fake API tests cover wide handles, return/error paths, argument validation, and
closure; an actual Windows test observes live parent/child and exited child.
That actual HANDLE test intentionally runs only on Windows, not as a substitute
for any existing cross-platform proof.

Only the process-user SID is memoized, per TaskSchedulerUserAdapter instance.
Task state and process liveness remain fresh on every observation. Invalid SID
results are not cached, and different adapter instances do not share the cache.
For N status calls through one adapter this removes repeated identity subprocesses:
2N SID-plus-state PowerShell calls become N+1. The regression verifies fresh task
state with one identity resolution; this is a call-count improvement, not a
measured whole-job speedup. No listener heartbeat or task-status cache was added.

### Test handoffs without larger deadlines

`some-implementations/gogurt/application/tests/test_listener.py` now waits for
both durable completed dispatch state and released active process/dispatch
custody before the restart/remount test requests shutdown. An output counter
file alone proves an action side effect, not durable listener completion.

The long-poll cooperative-stop test now propagates its run-thread failure into
the test instead of leaving it as an unhandled-thread warning. Existing stop,
kill, join, and polling limits are not enlarged.

PID-publication polling accepts only the classified Windows sharing/access
errors 32 and 5 as transient observations within the existing deadline. Other
permissions errors still fail. Tests cover that classification. This does not
retry the action, the test, the qualification, or a CI job.

### Independent, bounded failure evidence

`scripts/gogurt_pytest_evidence.py` is an opt-in pytest plugin registered by the
root conftest. `GOGURT_TEST_EVIDENCE_DIR` enables failure-only capture for setup,
call, and teardown failures, with a per-process report budget. It retains hashed
test identity, phase/error codes, sanitized heartbeat fields, read-only SQLite
state counts, and bounded Gogurt thread stack locations. It does not retain
source contents, local variables, arbitrary exception messages, raw databases,
configuration, action argv, or environment dumps. Symlink/nonregular heartbeat
and database paths are rejected, and the database query is read-only and bounded.

Tests exercise privacy, oversize inputs, missing/invalid state, budgets, write
failures, and a nested pytest failure whose evidence survives fixture teardown.
Capture errors do not replace the original failed test report.

`scripts/qualify_installation.py` independently attempts failure text, bounded
status command, native state, transition trace, and six fixed runtime log tails.
A failed or timed-out status command no longer suppresses later log capture.
Logs are bounded to 64 KiB per fixed file, with per-source collection errors.
Failures before listener setup also produce a minimal qualification failure
record when an evidence directory is requested.

Limitations: the pytest hook runs at report boundaries, before pytest fixture
teardown but not before a test's own finally block. It cannot recover already
lost worker stacks after a crash. Qualification subprocess output is bounded
when retained, not streamed into a memory-bounded capture implementation. These
are bounded diagnostics, not a proof that all failure-time evidence is complete.

### CI coverage and measurement

`.github/workflows/ci.yml` preserves the eight repository targets, sixteen image
targets, all three native platforms, twelve serial lifecycle repetitions per
platform, and the original failure/deadline semantics. Native pytest and staged
qualification have separate failure artifacts. Missing requested artifacts are
errors rather than warnings; names include source SHA, run ID, and attempt.
`make unit` failures also get a distinct bounded pytest evidence artifact.

Native pytest adds `--durations=20`. Staged qualification gains `--timings` and
retains its existing summary. Monotonic phase records include artifact staging,
each installation component, and each listener repetition, with outcome and
source/run identity. Timing is written on success and ordinary failure, not
just on a green run. Timing parents include their children: do not sum component
and lifecycle durations as independent work.

The harness already stages the whole candidate artifact set once, then reuses
it for all twelve lifecycles. It still tests independent install/idempotent
install/reinstall/uninstall operations, wheel-only consumption, exact managed
Python patch and locked package closure, native providers, and disposable mounts.
The first repetition still exercises extended lifecycle cases. No all-project
wheel requirement, native platform, test assertion, or repetition was removed.

`.github/workflows/reference-515.yml` is a read-only branch-specific caller of
the actual CI workflow at the exact pushed SHA. It is a validation convenience,
not a replacement integration workflow or permission to change required checks.

## Validation actually performed

The original exact source was obtained from a tracked-tree GitHub archive, not
reconstructed from code excerpts. The local working copy used a synthetic git
snapshot for diff generation; that local commit is not a repository source SHA.

Local diagnostic validation passed 192 tests with one Windows-only skip. That
run used an unlocked local Python environment with workspace source paths and
is not represented as installed-wheel or native-platform acceptance. Repository
Ruff checks and `git diff --check` also passed locally.

Locked GitHub preflight subsequently passed on Ubuntu 24.04 with managed
CPython 3.12.3 and uv 0.11.24:

- `make lint` passed, including the repository's REUSE, formatting, lint, and
  configured type-check gates.
- `mise x -- uv run --locked --all-packages --group dev python -m pytest -q
  some-implementations/gogurt/application/tests
  tests/unit/test_gogurt_pytest_evidence.py
  tests/unit/test_qualify_installation.py tests/unit/test_github_actions.py`
  passed: **192 passed, 1 skipped in 4.99 seconds**. The sole skip was actual
  Windows HANDLE proof on a Linux runner.

Evidence: [first-attempt preflight run 36855764298](https://github.com/nashspence/riverhog/actions/runs/36855764298),
job 110347752188, October 1, 2026. This checked the materialized working tree
under transport checkpoint `82e4f7370d07aa6682b9b693dbe3d155772bd3aa` and then
published implementation commit `feb7aa9cde14774bbe7cf27bbd9d473f30826c27`.
It is not falsely described as a full CI run on the final handoff commit.

The final read-only reference workflow invokes full CI after publication of this
record. The owning issue records the actual run and observed state for that
exact SHA; a launched or pending run is not a passing result. No failed job was
rerun to manufacture a successful first attempt.

## Validation not established and remaining work

This reference has not established the three consecutive full candidate
qualifications required by #515, final-candidate CodeQL, managed macOS consumer
proof, or release acceptance. A single green full CI run would not establish
those additional requirements. Do not borrow the audited base's green run as
validation of changed code.

The September 12 Linux dispatch-worker settlement failure remains unclassified;
this reference deliberately does not alter `_settle_worker` to conceal it. The
separate Windows PID-publication observation and false-death API semantics have
concrete fixes, but they are not evidence that the Linux worker root cause is
solved. Preserve the original stop/custody assertions when investigating it.

No end-to-end minimum CI wall time or percentage speedup is claimed. Compare
first-attempt, identical-coverage runs with the new phase data, separating tool
setup, staging, native pytest, ordinary lifecycles, and extended lifecycle work.
Report runner/image and cache conditions. Use exclusive durations or explicit
parent/child accounting. Optimize measured duplicate work before increasing
parallelism; native singleton services must not race on one user session.

Any future sharding proposal must preserve all required cases and repetitions,
use genuinely isolated native hosts, aggregate every required shard with a
fail-closed missing-result check, and retain exact candidate/attempt identity.
Removing platforms/repetitions or accepting partial-shard success is not an
optimization compatible with this handoff. No such sharding is introduced here.

## Publication integrity and integration guidance

The connected interface did not offer direct local-git import. The producer
supplied a reviewed patch, reconstructed remotely without substantive edits:
57,669 bytes, SHA-256
`f7e9217f0dfe710ccde0f4a9688532fc552d70458b4fc6841851b903c236c64c`.
The temporary publisher enforced checksum, byte counts, exact nine-file allowlist,
and `git apply --check --index`; CI YAML was published separately through the
connected interface. It ran locked preflight before a non-force push to this
reference branch only. The transport payloads and contents-write publisher are
removed from the final tree; history is retained for provenance. The remaining
reference workflow is read-only.

Review the final tree diff against the audited base, not a blind cherry-pick of
transport commits. Ten implementation/test/CI paths comprise the candidate;
`.reference/` and the reference-only workflow are handoff scaffolding. Reconcile
and land accepted changes through the then-current integration rail with its
required validation. The actual final GitHub SHA, not any local snapshot or
intermediate publication checkpoint, is the durable identity in #515.

## Research trail

Owning decisions and recorded regressions: [#515](https://github.com/nashspence/riverhog/issues/515).
Reference convention: [#903](https://github.com/nashspence/riverhog/issues/903).
Audited source: [main at 99179cbb](https://github.com/nashspence/riverhog/tree/99179cbb94a96d8ea2e633ab60c9e34be1763527),
particularly AGENTS.md, CI, listener runtime/tests, filesystem publication, and
staged installation qualification.

The base already had a first-attempt successful full CI run:
[36350218317](https://github.com/nashspence/riverhog/actions/runs/36350218317),
September 27, 2026. It is historical context, not candidate proof or a controlled
performance comparison.

Primary API references checked during this work:

- [Microsoft OpenProcess](https://learn.microsoft.com/en-us/windows/win32/api/processthreadsapi/nf-processthreadsapi-openprocess).
- [Microsoft WaitForSingleObject](https://learn.microsoft.com/en-us/windows/win32/api/synchapi/nf-synchapi-waitforsingleobject).
- [Microsoft CreateProcessW process security context](https://learn.microsoft.com/en-us/windows/win32/api/processthreadsapi/nf-processthreadsapi-createprocessw).
- [Microsoft CreateFile sharing semantics](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilea).
- [pytest hook reference](https://docs.pytest.org/en/stable/reference/reference.html).

The implementation tests operationalize those contracts; the references alone
are not substituted for native execution or issue acceptance.
