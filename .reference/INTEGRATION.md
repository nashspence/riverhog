# Integration notes for #983

The owning issue controls scope and acceptance. These are concrete implementation suggestions, not a parallel specification or a requirement to adopt the reference API.

## P00 — Small first integration slice

Use a separate authoritative worktree. Record its exact source SHA and any relevant source drift in #983. Compare `tests/unit/test_api_client_downloads.py`, `tests/unit/test_api_client_uploads.py`, root `conftest.py` and the qualification environment entrypoint with the pinned audit. Review the patch's placement under `scripts/`, consistent with test/qualification ownership; no runtime package may import it.

The patch adds `scripts/validation_timeouts.py` and `tests/unit/test_validation_timeouts.py`, and changes seven existing thread synchronization timeouts. It preserves concurrency, expected byte counts, identities, callback assertions and the heartbeat's scenario input. `ValidationTimeouts` captures an immutable factor, converts nominal budgets, checks backend capability and constructs a fixed monotonic deadline. It is not an automatic runner timeout, a background cancellation facility, a proof of product responsiveness, or a classifier of call sites.

First check applicability and review the diff; then run the following on the reconciled candidate with the repository-pinned tools, not on the unchanged reference tree:

```sh
RIVERHOG_VALIDATION_TIMEOUT_SCALE=1 mise x -- uv run --locked --all-packages --group dev   python -m pytest -q tests/unit/test_validation_timeouts.py   tests/unit/test_api_client_downloads.py tests/unit/test_api_client_uploads.py   packages/http-api-contracts/tests/test_control_budget.py
RIVERHOG_VALIDATION_TIMEOUT_SCALE=4 mise x -- uv run --locked --all-packages --group dev   python -m pytest -q tests/unit/test_validation_timeouts.py   tests/unit/test_api_client_downloads.py tests/unit/test_api_client_uploads.py   packages/http-api-contracts/tests/test_control_budget.py
```

The control-budget file is intentionally unchanged: its injected-clock boundaries must still mean the same thing. Add early validation of configuration at the actual direct-pytest and qualification startup boundaries before broad adoption; the patch alone only validates when a policy is constructed. A local `tests/conftest.py` cannot configure all sibling package tests, and a plugin loaded only by Make cannot cover direct pytest. Avoid an import-time policy singleton: tests of configuration need explicit isolated policies, while an invocation's live workers need a consistent captured value. Reconcile the new module with existing type-check ownership.

Post the adopted/reproduced/partially used/rejected disposition and the next concrete family in #983. No global source rewrite or separate design approval cycle is needed for ordinary implementation choices. Escalate a specific independent contract conflict rather than inventing a new product promise.

## P01 — Classify by purpose and establish deterministic witnesses

Use current pytest collection, Make recipes, `ci_unit.py`, `ci_qualification.py`, workflows and their non-pytest entrypoints to define maintained coverage. `scan_waits.py` helps locate syntax; it does not resolve authority, imports or control flow. Review helper calls, strings containing child programs, library defaults and generated fixture configurations. Keep decisions in the issue and durable intent in focused source-owned tests/comments, not another normative registry.

For each relevant duration, ask what would be wrong if the operation were correct but much slower:

- If only the observer's patience is exhausted, use the common validation watchdog.
- If expiration, lease duration, configured budget, cancellation order or ownership is the behavior being tested, preserve that semantic rule. Use fake/injected clocks for arithmetic and actual native/process/HTTP evidence where the contract requires it.
- If an arbitrary test-only duration is merely arranging a real scenario, establish readiness and scale related fixture timings coherently or replace the timing assumption. A timeout test is not automatically exempt from startup scheduling races.
- If it is a polling cadence, no-wait sentinel, workload size, algorithmic step count, safety policy or provider/resource bound, do not scale it just because it is numeric.

For metadata contacts, hold the worker until explicitly released and demonstrate that the caller's bounded path returns, the occupied origin remains unavailable and independent work can progress. Include a failure challenge that incorrectly frees the occupied capacity. Preserve a real trickled-body witness for total-contact behavior, but do not let its evidence depend on an unacknowledged 150 ms startup race or on finite trickling finishing before a scaled outer wait. Deadline arithmetic alone is not evidence of actual containment.

For serialization/negative waits, synchronize on the competing operation reaching its critical attempt, or observe the database's actual lock wait, then establish the required ordering after release. Merely submitting a future and checking `not done()` can pass when it has not run. Retaining an intentional negative observation window may be justified; classify and budget it rather than globally replacing expected `TimeoutError` calls.

For native-consumer termination, the current fixture writes a PID before sleeping while its parent calls `run(timeout=0.5)`. A slower interpreter may expire before writing that PID. Preserve the actual consumer-containment proof: use an appropriate readiness/supervision design or coherent arbitrary scenario allowance. Do not change production timeout semantics, replace the child with a mock, or weaken the assertion that it has actually stopped.

## P02 — Compose the same policy across execution boundaries

Recommend `RIVERHOG_VALIDATION_TIMEOUT_SCALE`: absent means 1; finite positive numeric values including fractions are supported. Malformed, explicitly empty, zero, negative, nonfinite and unrepresentable values are configuration errors, not fallback-to-one or disable-timeout requests. The reference intentionally rejects zero/None through its watchdog API; callers with genuine nonblocking or indefinite semantics keep those values out of it.

Resolve once at the invocation boundary. A nominal budget and an effective budget are different data. Across each subprocess/container boundary propagate either **factor + nominal values**, or **already effective values with an explicit consumer convention**. Never silently send an effective budget to another scaler. Keep the same value through xdist workers and recorded execution conditions. Test the actual child/container environment, not only a parent dictionary.

For the processing fixture, compute all nominal formulas first and scale the results once. In particular `nominal_admission = nominal_observation + 600`; scaling an already scaled observation and then scaling admission again is incorrect. File count, frames, bytes, quota and concurrency remain unchanged. Explicit test-generated service overrides may enlarge existing configuration values needed for the fixture, but runtime code must not learn this validation environment variable. Test-only controls must not leak into production compose defaults or image contents. `tests/Dockerfile` already includes scripts; other service images may not, so passing a validated effective value can be preferable to shipping tooling.

Use monotonic deadlines, not wall-clock timestamps. A fixed deadline consumes time across retries. Cap inner requests against remaining effective observation time when the API supports that meaning. The reference's `remaining(maximum_effective=...)` accepts a value already in effective seconds: do not feed the result into `seconds()` again. A transport's per-read timeout is not a whole-request wall-clock bound; do not claim stronger termination than the implementation supplies.

Integer-unit adapters round upward. In particular PostgreSQL and Playwright use zero as a disable sentinel in timeout settings; truncation must not turn a small positive value into no watchdog. Validate multiplication, monotonic representability and consumer bounds, such as `threading.TIMEOUT_MAX`. A backend capability rejection differs from silently clamping an environment request. The generic helper cannot infer every backend's maximum; adapters own that check.

Configure browser launch plus each created context's action/navigation defaults and any independent assertion timeout surface actually used. Configuring one page does not cover later contexts. Configure test-owned SQL limits transaction-locally and retain row/plan/work assertions while removing arbitrary execution-time success criteria. Audit Compose healthcheck attempts, intervals, start periods and stop grace together: enlarging only the command timeout does not necessarily enlarge readiness observation. Shared deployment compose files need test-only overrides, not altered production defaults.

## P03 — Cancellation, evidence and outer runners

Watchdog exhaustion does not prove that the worker failed, that a remote operation did not commit, or that cancellation completed. Release held fixture work in `finally`; join it and assert actual termination where appropriate. Avoid a failure path that blocks forever in executor context exit. If a deliberately broken concurrency witness can strand a non-daemon thread, isolate it under a test-owned process supervisor rather than pretending `future.result(timeout=...)` is a hard stop.

Do not retrofit a speed-goal kill deadline into `run_validation_child`. It intentionally forwards cancellation and waits for owner cleanup. Separate observation and diagnostic/cleanup allowances; an already exhausted observation deadline cannot safely be the only cleanup allowance. Any forced fixture-process escalation must be explicitly owned, preserve evidence and report incomplete cleanup honestly; it cannot authorize abandonment of production custody.

Extend existing evidence producers/consumers rather than creating a new validation runner. Candidate locations include `ci_qualification.validation_environment`, root pytest setup, `ci_timing.PytestTiming`/`execute_validation`, `ci_make` observed conditions and the relevant Actions evidence consumers. Keep required work definitions and advisory targets separate from environmental patience. Record scale, relevant nominal/effective budgets and the layer that expired; record real elapsed time/rates without division by scale. Preserve failed/interrupted attempts, exact source/run/owner identity and nonzero propagation. Do not replace known functional failures with a vague infrastructure status or treat incomplete evidence as success.

Workflow job timeouts are resolved outside an already running process. GitHub's job-level timeout expression contexts include `github`, `needs`, `strategy`, `matrix`, `vars` and `inputs`, not `env`; changing the runner environment cannot extend that job. Use validated pre-job planning/configuration and pass the resulting factor to the job, including non-primary workflows. Explicitly reconcile platform and independently owned resource ceilings. A budget that cannot fit must be visible, not silently truncated; environment scaling does not authorize larger provider retention/spending or restricted live qualification.

## P04 — Finish the migration, not only the prototype

Use the challenge matrix and the owning issue's acceptance. Cover every maintained family and retained independent-limit disposition, then run normal required gates and exact-source Actions checks on the integrated candidate. At nondefault scale, exercise representative real Python/subprocess/database/browser/shell/container/native boundaries with unchanged workloads. Test negative cases, not just a slower success. A passing helper or static scan does not establish repository-wide adoption.

Keep existing release/provider permissions. Restricted consumer wiring can be tested with safe fixtures/static checks; do not activate paid or release operations to close this issue. Record unavailable Windows/macOS/GPU/backend coverage as a gap, not a pass. No root-document rewrite or permanent reference-status registry is required.

## External API references checked for this handoff

These explain adapter hazards; they do not establish Riverhog contracts. Consult repository-pinned versions during integration.

- [Python futures shutdown](https://docs.python.org/3.13/library/concurrent.futures.html#concurrent.futures.Executor.shutdown): a timed-out caller has not terminated a running future; executor shutdown can still wait.
- [GitHub context availability](https://docs.github.com/en/actions/reference/workflows-and-actions/contexts#context-availability): job-level timeout configuration is not the runner's environment.
- [Playwright browser-context timeouts](https://playwright.dev/python/docs/api/class-browsercontext#browser-context-set-default-timeout) and [navigation timeout](https://playwright.dev/python/docs/api/class-browsercontext#browser-context-set-default-navigation-timeout): defaults are scoped and expressed in milliseconds.
- [PostgreSQL client defaults](https://www.postgresql.org/docs/current/runtime-config-client.html): statement/lock timeout semantics, units and zero-disable behavior.
