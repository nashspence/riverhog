# NON-AUTHORITATIVE EXTERNAL REFERENCE

Issue: #960
Convention: #903
Authoritative maintainer decision: https://github.com/nashspence/riverhog/issues/960#issuecomment-6013807376
Audited base: main @ a26e214f77ab806e4230125bf7aad93f8c3328da
Audited tree: dfd9f178581c0e1290a31584c824c9a286e55c98

Produced by: OpenAI ChatGPT
Model: GPT-5.6 Sol
Requested by: Riverhog maintainer
Published by: OpenAI ChatGPT via the connected GitHub integration

Purpose: implementation handoff for factorizing the remaining Stove0 processing
qualification bottleneck into independently required same-SHA proofs without
sacrificing the invariants owned by the current monolithic processing lane.

This branch is externally produced reference material for issue #960. Its
creation, publication, review, or request by the maintainer does not itself
authorize integration beyond the decisions recorded in the owning issue.

Authority remains with issue #960, subsequent maintainer decisions, and the
repository's normal integration rail. Reconcile this branch against then-current
authoritative source; do not apply it mechanically.

## Start here

Read `.reference/INTEGRATION.md` first. It is written as the integration job:
the target lanes, exact invariant ownership, file-level changes, rollout order,
and validation expectations are specified there.

`.reference/PROCESSING_COVERAGE.json` is the machine-readable coverage
contract for the proposed factorization. It exists so implementation and policy
tests can mechanically prove that no old processing invariant disappeared.

No production/workflow file is modified on this reference branch. That is
intentional. The accepted decision is about proof factorization; the integration
agent should implement it against current source and validate the exact result
rather than inherit an unexecuted shell rewrite from this producer.

## Key decision captured

The old #960 wording required the 16-input fixture, overlapping routes, two
producers, four serialized target jobs, durable admission boundaries, output
assertions, and restart/replay in one coupled lifecycle. The maintainer has now
superseded that Cartesian-product requirement.

The required same-SHA qualification set must still prove every one of those
properties, and at least one real FTP -> Riverhog -> Stove0 -> target -> Riverhog
end-to-end path, but independent dimensions may execute in independent jobs.

The target remote critical path is approximately 20-30 minutes under normal
GitHub-hosted conditions. Coverage controls that target; the target does not
authorize dropping an invariant.

## GitHub Actions resource basis

Current GitHub documentation checked for this handoff states:

- standard GitHub-hosted runners are free and unlimited for public repositories;
- public `ubuntu-24.04` standard runners provide 4 CPU / 16 GB RAM;
- GitHub Free permits 20 concurrent standard hosted jobs, with a 5-job macOS cap;
- larger runners remain billed even for public repositories.

References:

- https://docs.github.com/en/actions/reference/runners/github-hosted-runners
- https://docs.github.com/en/enterprise-cloud@latest/actions/reference/limits
- https://docs.github.com/en/billing/concepts/product-billing/github-actions

Issue #960 therefore intentionally carries no separate repository courtesy cap.
Keep cancellation of superseded runs; otherwise use standard public-runner
parallelism where it shortens the exhaustive gate.

## Validation performed

- Audited issue #960 and the superseding maintainer decision.
- Audited main at the exact SHA/tree above.
- Audited `.github/workflows/ci.yml`, `scripts/ci_qualification.py`,
  `scripts/test_compose_smoke.sh`, `Makefile`, and the relevant CI policy
  tests at the audited base.
- Traced the current processing lifecycle from FTP custody through admission,
  target execution, lineage/settlement assertions, metrics, and restart/replay.
- Audited `qualification/fixtures/stove0/recipes.yaml` and confirmed that the
  repository already owns `stove0.audio-archive/v1` as a single-route audio
  recipe and `stove0.conformance-media/v1` as the overlapping-route
  qualification authority.
- Confirmed the current hosted bottleneck evidence from #954/#960: unit
  factorization succeeded; `compose processing` remains the only material
  critical-path lane.
- Verified current GitHub public-runner resource documentation.

## Not validated / known limitations

- No Riverhog test suite or Docker qualification was executed by this producer.
- No proposed factorization has yet produced hosted timing evidence; the
  integration agent must measure it on the final exact SHA.
- The exact assertion API for observing "at most one Opus target job executing"
  should be chosen from the maintained target/operator surfaces during
  implementation. Do not add a test-only production contract merely to expose
  that observation.
- #948 and #953 remain independent authorities. This handoff must not establish
  new scheduler, target-concurrency, recipe-language, or production semantics.
- Native GitHub Development linkage between issue #960 and this reference branch
  is not available through the current publication interface. Per #903, record
  that limitation in the owning issue; a later capable publisher may add the
  link without changing this handoff identity.
