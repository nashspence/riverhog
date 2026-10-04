# NON-AUTHORITATIVE EXTERNAL REFERENCE

Issue: #954
Convention: #903
Audited base: main @ 29d16e99d52bda0f308495fd7be519c1e321b735
Audited tree: 2aa3a5547b11dc21e79825755888210075657faf

Produced by: OpenAI ChatGPT
Model: GPT-5.6 Sol
Requested by: Riverhog maintainer
Published by: OpenAI ChatGPT via the connected GitHub integration

Purpose: research and implementation handoff for reshaping Riverhog's local/remote CI gates around exhaustive same-commit coverage, bounded coarse-grained parallelism, exact-input reuse, and resource-friendly use of standard GitHub-hosted runners.

This branch is externally produced reference material for the owning issue.
Its creation, testing, publication, native issue linkage, or request by an
authorized repository actor does not itself establish or extend an accepted
design, contract, release requirement, or authorization to integrate these
changes.

Authority remains with issue #954, subsequent maintainer decisions, and the
repository's normal integration rail.

The exact reference commit recorded in issue #954 is the handoff identity;
this branch name is navigation only. Reconcile this material against
then-current authoritative repository state and the current owning-issue
decisions; do not apply it mechanically.

## Contents

- `CI_GATE_RESEARCH.md`: observed CI evidence, resource constraints, design principles, and recommended topology.
- `CI_GATE_INTEGRATION_MAP.md`: file-level implementation map, sequencing, experiments, and validation expectations.

No production or workflow file is modified by this reference branch. That is
intentional: the observed bottlenecks include expensive session fixtures and
stateful compose/restart proofs, so an unexecuted workflow rewrite would look
more authoritative than the evidence supports. The integration agent should
use this branch as researched design input and implement against current
authoritative source.

## Validation performed

- Audited `.github/workflows/ci.yml`, `Makefile`, `AGENTS.md`,
  `release.toml`, root/test fixtures, `scripts/_compose_env.sh`,
  `scripts/test_compose_smoke.sh`, and `scripts/test_runtime_compose.py`
  at the audited base.
- Read the complete GitHub Actions log for CI run #903 unit job and extracted
  slow-test/setup evidence.
- Read the complete GitHub Actions log for CI run #903 compose-smoke job and
  inspected its build/lifecycle behavior.
- Compared required-job durations for successful main CI runs #899, #901,
  and #903.
- Audited the active release/v1 ruleset and its required status contexts.
- Verified current GitHub documentation for public standard runner resources,
  Free-plan concurrency, cache storage, artifact storage, and public-repo
  standard-runner billing.
- Verified current pytest-xdist distribution/session-fixture behavior relevant
  to the proposed unit partitioning.
- Verified #903 reference-branch requirements and its current comments.

## Not validated / known limitations

- No Riverhog test command was executed by the producer. The producer's local
  execution environment did not provide the repository's mise/Docker toolchain,
  and this reference intentionally makes no production changes requiring a
  synthetic validation claim.
- No candidate sharding topology has been benchmarked. The integration agent
  must measure fixture duplication and wall time rather than accept the
  suggested shard count on faith.
- No compose scenario has yet been extracted from the monolith. Candidate
  boundaries in the research are semantic starting points, not accepted cuts.
- Native GitHub Development linkage between issue #954 and this branch could
  not be established through the available publication interface. This
  limitation is recorded in the owning issue handoff comment as required by
  #903.
