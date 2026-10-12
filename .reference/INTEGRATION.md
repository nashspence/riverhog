# Integration route for #968

Concrete implementation guidance, not a second scope or decisions document.
The [owning issue](https://github.com/nashspence/riverhog/issues/968) controls.

## P00 — begin with the existing restart attribution path

Use a separate authoritative worktree at the actual source chosen by integration.
Read current AGENTS and #968, then reconcile the focused seams below. Do not
assume the supplied base is post-#961/#963, and do not merge reference branches.
The reference's Python example is a counterexample set to translate, not a
production module to adopt or another declaration registry.

Record the exact source/ref, clean/dirty state, applicable #963 implementation,
and first useful slice in #968. Proceed with routine code and test choices.
Report only a specific prerequisite, missing owner declaration or semantic
conflict that actually prevents that slice; no fresh broad design study is needed.

**First deliverable:** carry the purpose and exact scope of the existing narrow
SQLite/ASGI event-cursor witness through collection, execution, evidence and its
actual qualification consumer. Include one policy and one tooling check in the
same report as negative controls. A source annotation alone is not this slice.

### Read these source seams, not the whole repository

All paths below refer to audited main `152d095038df0c0d0ed642cf265d47b11c65a754`.
Use source-seams.json to detect drift, not to decide semantic classification.

| Seam | Existing entry point | Integration use |
| --- | --- | --- |
| Collected execution universe | `scripts/ci_unit.py::Collection`, `partition`; `Makefile` canonical roots | Preserve exhaustive nodes and shard locality; do not derive purpose from shard names. |
| Automatic result transport | `scripts/ci_timing.py::PytestTiming`; `scripts/ci_qualification.py` | Extend/reuse actual #963 adapters and work identity rather than build a competing runner. |
| Real narrow witness | `tests/unit/test_event_cursor_restart.py::test_event_cursor_continues_across_process_restart` | Three parameterized applications; assertions concern persisted SQLite/ASGI process restart, not deployed-image or PostgreSQL recovery. |
| Existing attribution | `tests/operation_observer.py::pytest_runtest_logreport`, `timing_evidence` | Attribution currently follows a passed non-xfailed call; the enclosing failed session is rejected downstream. Preserve both boundaries. |
| Actual claim consumer | `scripts/operation_qualification.py::_event_cursor_restart_claim`, `_load_operation_timings`, `evidence` | Retain narrow success separately from the four broader unestablished claims. Do not manufacture a passed release lifecycle. |
| Actual release predicate | `.github/workflows/release-qualification.yml`, step `Verify exact-SHA operation evidence` | Exercise its current predicate against positive/negative records without running release operations. |
| Non-product controls | `tests/unit/test_security_policy.py`; `tests/unit/test_contract_review.py` | Document-content and comparison-tooling assertions must not count as proof of the cursor claim. Their required failures remain failures. |
| Candidate-only bindings | `scripts/extent_witnesses.py::bind_segmented_decisions` | Symbol existence is a candidate association, never an executed result. |
| Normative/audit separation | `scripts/contract_atlas/records.py::build_records`; `scripts/contract_atlas/discovery.py::_link_operation_qualification` | Keep evidence-purpose/result changes in audit-owned fields and preserve exact subject links. |
| Existing regression guards | `tests/unit/test_ci_unit_partition.py`, `test_operation_qualification.py`, `test_contract_records.py` | Extend coverage and failure-propagation guards instead of replacing them. |

Resolve the actual owner declaration, subject and demonstrated assertion before
emitting product-support metadata. A symbol or schema-shaped field is not itself
an independent promise. If a relevant general promise lacks a discovery route,
record the owning-contract finding rather than invent a synthetic identifier
or silently reclassify the witness as implementation-only.

### Recommended first consumer cases

Use existing operation tests, fixtures and source/run checks. Extend the actual
collector/observer/report/consumer path, not only a fresh helper's unit tests.

| Case | Required observation |
| --- | --- |
| Existing restart fixture succeeds | Its scoped local witness is attributable; deployed/broader claims remain unestablished. |
| Same report includes passing policy and tooling checks | They remain visible but add no cursor/lifecycle proof. |
| One node has policy and product assertion groups | Only the demonstrated product group supports the matched claim. |
| Successful expected rejection assertion | Can support the exact rejection promise, not a successful positive lifecycle. |
| Passed call, failed teardown/session | Keep diagnostic observation; reject it as successful required-run qualification. |
| Skip, xfail, xpass, interruption or missing group result | Do not promote to behavioral qualification. |
| Wrong parameter, source, run, work, conditions or claim scope | Reject mismatched evidence or leave the required claim unestablished, as the existing consumer requires. |
| Renamed/removed/unclassified check or new discovered subject | No stale binding, silent noncontractual default or inherited unrelated proof. |
| Goal absent, severe timing miss or unavailable comparator | No target-driven failure, hold or approval condition; independent failures remain. |
| Audit-only purpose/result change | Normative Closure meaning/digest stays unchanged. |

The synthetic module supplies a subset of these boundaries. It deliberately does
not implement real discovery, wire parsing, trusted artifact custody, collection,
run aggregation, rendering, arbitrary scope implication or release permission.
Translate examples into production regressions; do not use passing examples as
claims that the current product already satisfies these cases.

## After the first slice

**Classification coverage.** Extend existing collection/entrypoint discovery to
maintained pytest and non-pytest work. Prefer explicit source-local purpose and
reviewed inheritance where correct, with assertion-level overrides for mixed
checks. Preserve unresolved status until reviewed. Do not infer contractuality
from names, mock usage or component role. Keep requiredness independent.

**Bound evidence and consumer parity.** Generalize only the records needed by
the next real owner. Consume source-owned contract identities and existing
#873/#963/#964 witnesses. Preserve node parameter, work/conditions and selected
run/attempt identity; do not union stale retries into a pass. Distinguish counts
of checks, witnesses, obligations and displayed associations. Keep all existing
required validation failures even when their subject is non-product.

**Human reading and completion.** Show purpose, actual result, scope and limits
at the existing local/CI summaries and normal Audit reading path. Validate actual
rendered output and the release consumer with non-product/missing-proof controls.
Finish the issue's maintained-universe accounting; do not require proving every
honestly exposed product obligation to finish this accounting correction.

No native dependency or milestone is established by this route. Coordinate with
the actual #963 shared foundation when touching its transport. #873 retains
operation proof, #964 growth, #469 final release audit, and #961 onboarding.

## Validation to run during real integration

Preserve current AGENTS requirements and use locked tools. Start with the real
slice plus `test_ci_unit_partition.py`, `test_operation_qualification.py`,
`test_contract_records.py` and any changed transport/rendering tests. Then run
applicable full local and independent exact-pushed-SHA checks. Compare normative
Closure identity for classification-only edits. Required built/platform tests
stay with their owners; this handoff does not authorize provider/release actions.

Before a context reset or hand-back, post to #968: exact source/ref and worktree
state; changed files and actual contracts, if any; commands/results/artifact
identities produced; unresolved classification or proof scope; coordination or
specific blocker; cleanup state; and the next concrete command/slice. Keep that
progress in GitHub, not in tracked main planning or duplicate policy files.
