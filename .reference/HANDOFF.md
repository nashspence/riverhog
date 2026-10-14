# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue: [#955](https://github.com/nashspence/riverhog/issues/955)
Convention: [#903](https://github.com/nashspence/riverhog/issues/903)
Reference branch: `reference/external/955-tag-subscriptions`
Current audit: `main` @ `e0036f80410a83cc5eaa9d63c17650ffae72e2bd`
Current audit tree: `b109c466311b6bc54cfd0cf5cb08a2da55cffff1`
Historical handoff: `17a61b19bbdb9f254b40f4dbaca4f18abbf4b2cf`
Historical audit: `6fd9d9e3332e6b62b7d72e2c7e13490c3adad9ab`
Prepared: 2026-10-14

Produced by: OpenAI ChatGPT
Model: GPT-6 Astra Pro
Configuration/effort: not exposed
Requested by: Riverhog maintainer in this conversation
Published by: OpenAI ChatGPT through the connected GitHub interface

## Authority and handoff identity

This is external reference material, not an integration branch or an accepted
production contract. The owning issue and subsequent decisions own scope and
acceptance. Earlier conversational calibration is not another authority. Reconcile
against current main and use the normal integration rail. Do not mechanically
merge `.reference/`, import the model into production, copy its example DDL, or
copy design inventories onto main.

The revised reference is a descendant of the historical handoff and the current
audited main. Its tree outside `.reference/` is exactly the current audit tree.
Production differences against the old reference are only the audited main
refresh, not feature implementation. Publication preserves both histories and
uses no force push. The actual remote commit SHA recorded on the issue is the
handoff identity; branch spelling is navigation. Publication may reconstruct the
exact tree with GitHub Git objects; no unpublished local commit SHA is claimed
as the published reference.

The 2026-10-14 issue supplement and this handoff supersede the original reference
on extent/growth/completion/capacity details, not its destination-oriented hard-cut
architecture. The old handoff remains available at its exact historical commit.
No compatibility state, version bump, parallel materializer or second tag follower
is proposed. State owner: `a-riverhog-cli-materialization`.

## Read and use

Read the owning issue's latest supplement, then
[tag-subscriptions/GROWTH.md](tag-subscriptions/GROWTH.md) and
[the integration map](tag-subscriptions/INTEGRATION.md).

The supplement specifies conditional cheap idle reconciliation versus explicit
integrity audit, source-owned growth dimensions/costs, bounded range and metadata
turns, independent resource lanes, indexed inspection, compact lifetime identity
state, conservative replay guards, bounded subordinate cleanup, queue/storage
pressure and byte-bounded logs. It uses current repository growth/extent machinery
rather than creating an alternative acceptance registry.

The original `model.py` still supplies the semantic/transaction/placement examples.
It now retains one observation per distinct collection rather than per generation
and consumes at most one page plus one lookahead before rejecting a large iterator.
`growth_model.py` supplies separate small queue/query/cleanup/byte-turn examples,
not a second tag state machine. It attaches three declarations to actual SQLite
query/cleanup bodies through the real `riverhog-growth-contracts` library.

`test_model.py` retains the original 45 tests. `test_growth_model.py` adds 26 tests
for measured indexed work, mutation sensitivity, state growth over 100 rebaselines,
capacity rollback/reopen, retry fairness, subordinate cleanup, log bounds, scratch
reservation reuse and exact byte turns. `placement-vectors.json` is unchanged.
`run_growth_checks.py` reproduces the reference tests and emits scoped primitive
evidence. `growth-evidence.json.gz` retains the actual reference execution samples,
declaration digests and subject-file digests; it is not a production qualification
artifact and does not assert a clean integrated source/execution rail.

## Validation actually performed

A full Git-history clone was obtained through the read-only clone gateway. Its
archive SHA-256 verified as
`273f68420f48b1af8349cafd2c9a82c6fca891174ac0164466ad9cd1688e712a`.
The checkout HEAD/tree were verified against the audit above; ZIP executable
modes were restored from Git metadata before checking cleanliness. The revised
reference was prepared in a separate worktree of that exact commit.

Environment: Python 3.13.5; SQLite 3.46.1; existing container pytest. Current
repository source for growth/canonical JSON was imported directly. The missing
locked dependency `rfc8785==0.1.4` was retrieved as upstream tag source; its two
Python files matched Git blobs `3137d3326b98938affadb1be711ee411eb2ab86e` and
`5a1f9d919643fa3bcaa0999ea66d9c535568c42a`. It is external validation environment
material, not vendored or published in this reference.

```sh
python .reference/tag-subscriptions/run_growth_checks.py --output /tmp/955-growth-evidence.json
python -m compileall -q .reference/tag-subscriptions
git diff --check
```

Result: **71 reference tests passed**; compilation and whitespace checks passed.
Actual reference SQL observations include 65,537-row sparse/idle/deep-keyset
fixtures, one-unit deletion of a 4,097-child parent, and a 1,001-row full traversal.
Negative controls remove an index and substitute deep OFFSET; the declared work
envelopes reject both. One hundred rebaselines of the same 97 collections retain
97 observations and 97 immutable identities, not 9,700 observation rows.

Focused maintained source tests were also run with source paths configured and
pytest `-c /dev/null --confcutdir=/mnt/data/955-validation` to avoid unrelated root
fixture setup: **30 tests** in `tests/unit/test_growth_declarations.py`, and
**187 tests** across `packages/riverhog-client/tests/test_catalog_following.py`,
`test_catalog_sync_engine.py`, `packages/riverhog-materialization/tests/test_planning.py`
and `tests/unit/test_shared_checkpoint_sha256.py`. All passed. The latter invocation
reported one unknown `growth_witness` marker warning because normal repository
pytest configuration/evidence hooks were not loaded. These are focused semantic
unit results, not production growth-witness/required-gate qualification.

## Not validated and limitations

- Repository-locked Python is 3.12.3; this validation used 3.13.5. Full locked
  lint/unit/distribution/build/linux-qualification, Compose/PostgreSQL/recovery,
  installed-wheel and native platform/Actions qualification were not run.
- No production CLI/schema/SDK feature implementation, real HTTP range transfer,
  scratch/hash custody, filesystem publication/locks/ACLs, native schedule or
  release/configuration wiring was implemented or qualified.
- The old semantic model intentionally does not reproduce the full wire models.
  The operational model's SQL schema and counters are narrow examples. Native
  instruction bounds apply only to their declared SQLite/fixture scope; they do
  not measure B-tree page reads, total RSS, network latency or OS filesystem cost.
- The byte-turn tests use in-memory streams. They do not prove real range integrity,
  resumed hash custody or bounded retry amplification of a production materializer.
- Lifetime distinct identities, required outputs and intentional suppression state
  genuinely grow. The design does not promise constant disk use, unique timestamps,
  bounded calendar-directory fan-out or completion under perpetual overload/failure.
- The current connected interface has no native Development linked-branch mutation.
  The issue records that relationship as unestablished; Markdown links are not a
  substitute. A capable publisher can add it without changing this commit.

The issue's publication comment records actual remote commit/tree verification and
Actions observations separately. No main/release ref, deployment, provider
provisioning, release publication, issue closure or PR is part of this handoff.
