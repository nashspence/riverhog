# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue: [#955](https://github.com/nashspence/riverhog/issues/955)
Convention: [#903](https://github.com/nashspence/riverhog/issues/903)
Audited base: `main` @ `6fd9d9e3332e6b62b7d72e2c7e13490c3adad9ab`
Audited base tree: `7a24614f2b462e27fa6e187138eef6588495dc22`
Reference branch: `reference/external/955-tag-subscriptions`
Prepared: 2026-10-04

Produced by: OpenAI ChatGPT
Model: GPT-6 Astra Pro
Configuration/effort level: not exposed
Requested by: Riverhog maintainer, in the current conversation
Published by: OpenAI ChatGPT through the connected GitHub publication interface

## Purpose and authority

Supply a recommended destination-scoped materialization/subscription design,
executable transaction/placement examples, and a concrete integration map. The
owning issue records the recommended scope and acceptance criteria; prior chat
alignment is not an additional authority. This branch changes only `.reference/`.
It does not implement the production feature or change a deployed contract.

This branch is externally produced reference material for the owning issue.
Its creation, testing, publication, issue linkage, or request by an authorized
repository actor does not itself establish or extend an accepted design,
contract, release requirement, or authorization to integrate these changes.
Authority remains with the owning issue, subsequent maintainer decisions, and
the repository's normal integration rail. Reconcile against then-current main
and issue decisions; do not apply this branch mechanically or copy the reference
Markdown inventories into main. Accepted reuse is an explicit integration act.

The exact published commit recorded in the owning issue is the handoff identity;
the branch name is navigation. Publication reconstructs these exact UTF-8 files
on the audited base using GitHub Git objects. There is no producer-side Git
commit being claimed as imported. The issue publication comment records the
actual remote reference SHA and verification, not an invented local commit.

## Contents and first integration step

- [INTEGRATION.md](tag-subscriptions/INTEGRATION.md): source/owner map, ordered
  implementation steps, existing witnesses to extend, policy and release gates.
- [model.py](tag-subscriptions/model.py): dependency-free, reference-only model
  of exact tag response checks, observed match/revision reduction, root-local
  temporal placement, transactional intent/cursor acceptance and reset fencing.
- [test_model.py](tag-subscriptions/test_model.py): 45 executable tests covering
  those invariants, restart/rollback, collisions, suppression, explicit caught-up
  signaling and idle schedule status.
- [placement-vectors.json](tag-subscriptions/placement-vectors.json): six golden
  date paths and negative syntax/calendar cases; not a production format schema.

Start by reading #955 and the integration map, then reproduce the useful tests
at their real owners. Use the existing public `CatalogFollower`; add explicit
`caught_up` information to its public batch, and extract generic tag/match logic
into `riverhog-client`. Do not import this model, reuse its simplified DDL as
production state, or establish an alternate scheduler/state machine from it.

## Validation actually performed

Environment: Python 3.13.5; standard-library unittest and SQLite.

```sh
python -m unittest discover -s .reference/tag-subscriptions -p 'test_*.py' -v
python -m compileall -q .reference/tag-subscriptions
```

Result: **45 tests passed**; Python compilation succeeded. These are executable
model results only. They include 1,001 logical collections advanced across
bounded pages, transaction rollback/reopen, two separately prepared database
writers, stale generation rejection, exact timestamp collisions, and preservation
of nanosecond placement. The golden vectors are read by the test suite.

Audited source through the GitHub connector at the exact base: repository policy,
architecture, catalog follower, publication boundary, timestamp/destination
utilities, existing CLI, Stove0/Gogurt precedents, release and CI ownership.
The publication comment records remote blob/tree/base verification separately.

## Not validated and known limitations

- No full repository checkout was available: direct Git network access failed
  and the clone gateway reported exhausted clone slots. No repository code was
  executed. No production lint, unit, distribution, image, Compose, PostgreSQL,
  independent-recovery, installed CLI or native platform qualification was run.
- No production schema/API/SDK/CLI changes, filesystem locks/publication,
  retrieval/provenance transfers, permission/ACL handling, credentials, native
  schedule installation or structured runtime ledgers are implemented here.
- The model is intentionally smaller than the real wire contracts. It uses a
  simple JSON encoder for its own scalar documents, not Riverhog canonical-model
  validation, and does not prove complete CatalogFollower behavior. Prepared
  SQLite writer rejection is not a native filesystem concurrency proof.
- Collision-only naming is stable within a root, not independent of reservation
  order across roots. Date partitioning does not bound all possible directory
  fan-out. Nanosecond timestamps are not unique collection identities.
- The audited CI push filter names main/release/v1, not reference branches. No
  passing repository/Actions claim is made for this reference. Production proof
  belongs to the exact integration SHA, not these model results.
- The connected publication interface does not expose GitHub's native issue
  Development-branch mutation. That relationship remains unestablished and is
  explicitly recorded in #955 under #903. A capable publisher may add it later
  without changing the reference commit; Markdown links are not a substitute.

No main/release branch updates, pull request, deployment, provider provisioning,
release publication or issue closure is part of this handoff.
