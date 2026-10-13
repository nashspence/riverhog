# Integration route — current source, one slice at a time

This is an implementation handoff, not another scope authority. Read current
AGENTS and #968. Source below is `e43df5b5acce5d730c28b48b084e87445fbaac3b`.
Do not mechanically apply the reference or treat its sample states as a required schema.

## P00 — establish the real end-to-end path

**Deliverable:** one real scoped claim, policy/tooling controls, actual execution,
machine output, and the existing operation consumer. Keep all four broader
operation claims unestablished unless their owner independently supplies proof.

Read `scripts/ci_unit.py::Collection`, `tests/unit/test_event_cursor_restart.py`,
`tests/operation_observer.py::pytest_runtest_logreport`,
`scripts/operation_qualification.py::_event_cursor_restart_claim` and
`tests/unit/test_operation_qualification.py`.

The real test has three parameterized applications and four groups of expectations:
retained event identity/content/order; one-event progression; newly appended events;
and empty terminal-page semantics. It creates fresh Python processes over SQLite,
with official clients using real ASGI routes. It is not deployed-image, PostgreSQL,
crash-recovery or durable consumer-checkpoint evidence.

Resolve the existing owner declaration and asserted meaning first. Reuse its
source identity; do not invent a normative ID to match a test name. Add purposeful
attribution at the smallest meaningful group. A whole test can be a group when its
scope is coherent. This does not require per-assertion instrumentation everywhere.
Use `test_security_policy.py` and `test_contract_review.py` as controls *for this
cursor claim*, not a claim that tooling can never have external contracts.

Run the collected parameterized witness with the actual observer and existing
consumer, adding malformed/missing/non-product evidence and failed-teardown cases.
Replay the actual release predicate without invoking release/provider operations.
Record exact source, current foundation, changed files, commands and first result
in #968. Proceed with routine choices; escalate only a specific authority conflict.

## P01 — cover the real universe without inventing a parallel one

**Deliverable:** reviewed purpose for every maintained check, exact collected nodes
and non-pytest work identities, mixed cases, and bidirectional relationship accounting.

Use `Makefile`, `pyproject.toml`, `scripts/ci_unit.py`,
`scripts/ci_qualification.py::validation_workloads`, and
`scripts/ci_make.py::maintained_make_targets`. Existing execution groups preserve
fixture locality; their names do not determine normative purpose.

Prefer explicit local annotations and reviewed module defaults with narrow overrides.
A newly added, unsupported or ambiguous construct must remain visible rather than
inherit an unreviewed implementation-only classification. Reconcile source metadata
with actual pytest collection, including class methods, parameters and applicable
platform/integration/browser selections. Static AST symbol discovery is not that
universe: current `validation_witnesses.py` only scans selected roots and top-level
literal growth markers. Keep valid narrow growth bindings while bridging full
collection; do not silently generalize its scope.

Non-pytest built/script/provider work uses the maintained owner's execution boundary.
A build, static check or policy check can be required without proving product behavior.
Classify independent external tooling promises under their actual owner where real.

In both directions, surface promises without sufficient witnesses, intended product
checks without resolved promises, stale nodes/parameters and retired expectations.
Dispositions are bind/correct or a reviewed owning-contract finding; never silently
restore an old promise, manufacture one, drop a required check, or count an unmapped
check as proof. Keep findings/decisions in GitHub and generated audit, not a second
normative registry. Classification coverage is not behavioral coverage.

## P02 — extend the integrated evidence foundation

**Deliverable:** purpose, oracle provenance, exact scope and assertion outcomes travel
through the real #963 transport and actual #873/#964 adapters without changing gates.

Use `scripts/ci_timing.py::PytestTiming`, `scripts/validation_witnesses.py`,
`scripts/validation_evidence.py`, `scripts/ci_make.py::owned_execution_spans`,
`validate_make_receipt`, `scripts/ci_execution.py`, and
`scripts/ci_actions.py::verify_ci_artifacts`. Extend existing formats/adapters only
where needed. No second runner, instrumentation pass, artifact store or target registry.

Keep discovery, candidates, execution, evidence integrity, sufficient scoped support,
required validation and advisory observations separate. A growth collector's `pass`
currently means the owned assertions executed with valid observations; its own
contract says independent declarations and Audit bindings are still required for
product conformance. An operation HTTP success and a source-linked extent candidate
have different meanings. Preserve them, not just their spelling.

Retain source/work/conditions, node parameter, run/session/attempt and artifact origins.
Do not union convenient successes across stale retries or broaden a local scope.
Preserve deliberate supported reuse of exact owned job completions across an Actions
rerun, with their origin attempts; do not turn "same attempt" into a blanket ban.
Follow the selected qualified Make owner and descendants. A failing negative-test
child observation is not an independent required owner; a genuine owned failure
cannot be excluded by relabeling it as a fixture. Skips/xfails/xpasses/incomplete
runs do not prove required behavior. A valid report of failure is not corrupt
merely because it reports failure, but the failure must survive every consumer.

Prove against the current raw-payload hashing witness in
`tests/unit/test_raw_ingress_manifest.py` as well as P00. Preserve its narrow measured
byte-hashing scope; #964 owns additional growth declarations and composition.

## P03 — prove meaning at consumers and in human output

**Deliverable:** one exact accounting model drives local/CI summaries, Audit routes
and each actual qualification consumer, with no stronger downstream claim.

Use `scripts/contract_atlas/records.py::build_records`,
`discovery.py::_link_operation_qualification`, renderer modules reached by those
records, `scripts/extent_witnesses.py::bind_segmented_decisions`,
`operation_qualification.py::evidence`, `ci_actions.py`, and
`.github/workflows/release-qualification.yml`.

For each relationship show the owning expectation, assertion/group, conditions,
actual outcome, limitations and remaining requirement. Declarations/structural
checks may prove a structural promise, but not an unrelated runtime lifecycle.
A reused normative parser/schema or independently checked vector is not forbidden;
a second implementation is not mandatory. Ask whether the check would catch the
relevant error rather than compare two equally wrong projections.

Prove legitimate agreement-only and negative/rejection promises positively. Use
selected corrupt-output or bypassed-assertion tests to ensure the actual consumer
loses support or a focused guard fails while unrelated scaffolding stays green.
No automatic semantic proof checker or repository-wide mutation campaign is required.

Display counts separately for selected/collected checks, executed assertions,
candidate associations, unique supported obligations and unestablished obligations.
Repeated links, worker/controller copies or parameter multiplicity do not create
extra contractual proof. Preserve useful diagnostic observations from failed runs
without making that run eligible. A metadata or rendering-only change leaves
normative Closure meaning/digest unchanged. Fix status labels where misleading;
no compulsory source/file/shard renaming.

## P04 — onboarding and finite closeout

**Deliverable:** approved guide wording, maintained-universe accounting, complete
consumer challenges and final source-bound integration evidence.

Apply [onboarding.patch](onboarding.patch) deliberately against current source;
[AGENTS.section.md](AGENTS.section.md) is the exact approved wording from #968's
maintainer decision. Preserve unrelated sections, the inline link, bold conclusion,
2,100-word limit, whole-document integrity and all existing guide-policy checks.
The new guide-policy test is not a product-conformance witness.

Use [ACCEPTANCE.md](ACCEPTANCE.md). Reconcile the final collector/entrypoint inventory,
all current producers and consuming routes, and all existing mandatory coverage.
Finish classification and relationship dispositions; do not finish after P00 or
wait for every honestly exposed product proof gap to close. Update current #968
with the final source and evidence per acceptance item, remaining owner-linked
proof gaps, unchanged contract identity or separately approved changes, and the
actual required local/CI outcomes. Stop once those criteria hold; another speculative
ontology pass is not a gate.

Focused commands use the maintained locked environment and actual current roots.
Existing starting tests include `test_ci_unit_partition.py`,
`test_operation_qualification.py`, `test_validation_witnesses.py`,
`test_validation_evidence.py`, `test_ci_actions.py`, `test_contract_records.py`,
`test_entrypoints.py`, and the actual Make/observation regressions affected.
Then preserve AGENTS' full local and independent exact-pushed-source requirements.
This does not authorize providers, protected branch changes or release publication.

Before changing owners or handing back, post exact source/ref and worktree state,
changed files/contract effects, commands/results/artifact identities, residual
classification or proof findings, cleanup state and the next concrete action.
The next agent should continue a named slice, not restart research from the chat.
