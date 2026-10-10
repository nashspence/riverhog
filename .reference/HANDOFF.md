# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue: #964 — large-collection scalability
Convention: #903
Audited base: `main@8425e672c60285a549ca4d5aaba15ecbd99e5bf3`
Audited tree: `d18b813a489955b085094765f98282ea2e2276d0`
Previous reference / intended direct parent: `703ff38fa6c0f50d9954b78503e16c9958b2f46a`

Produced by: OpenAI ChatGPT
Model: GPT-6 Astra Pro
Configuration: no separately exposed effort setting recorded
Requested by: Riverhog maintainer
Published by: OpenAI ChatGPT through the connected GitHub integration

This revision is source-controlled research, a reproduced algorithm audit, and an
integration guide. It is **not a production patch**, successful full qualification,
or approval of a new public contract. The exact newly published reference SHA in
#964 is the handoff identity. The branch name is navigation. Preserve the previous
reference in history; this revision supersedes its start-here and sequencing
instructions where they differ, and retains its preliminary research evidence.

## Integration agent: start here

1. Read the current owning-issue decisions, especially the explicit
   [#960 performance-shortfall disposition](https://github.com/nashspence/riverhog/issues/960#issuecomment-6102101195)
   and [#963 coordination clarification](https://github.com/nashspence/riverhog/issues/963#issuecomment-6102103067).
2. Read `SUPERLINEAR_INTEGRATION.md`. It gives the nine known local algorithm
   candidates (the earlier output-decision lookup plus eight additional findings),
   distinguishes the conditional discovery-representation risk, and maps each
   candidate to its owner, acceptance proof, and unsafe shortcuts.
3. Reproduce `superlinear/` on a separate exact audited-base checkout. Reconcile
   the findings against the actual integration source before changing runtime
   code. Drift is not permission to remove a guard and call old evidence current.
4. Consume #963's shared measurement definition, not a new objective registry.
   Extend the candidate workloads in `WORKLOADS.json` only through #964's approval
   checkpoint before installing new maintained targets or large proof cadence.

## Current disposition and sequencing

The maintainer now authorizes #960 to close **with its identified unmet 600-second
objective**, after applicable complete functional validation, exact-source evidence,
and fixture cleanup. This is a specific timing exception, not a pass or a waiver
of correctness, missing evidence, or other targets. Stop expanding #960 through
performance-only closeout iterations. Preserve its runnable profile, the 600-second
report-only target, and unchanged workload/interval. #964 owns the residual latency
and algorithmic scaling work, including whether the full objective is achieved.

#963 and #964 may be prepared and handled as one coordinated integration effort.
Their responsibilities stay distinct. #961 remains #963's prerequisite; land the
#963 measurement foundation before dependent #964 production/target integrations.
Reconcile against that exact integrated baseline. Keep #964's explicit maintainer
workload/condition/target/design checkpoint, especially for representation, identity,
state or protected-boundary changes. Do not use a reference branch as authority.

## What this revision adds

- `SUPERLINEAR_INTEGRATION.md`: prioritized implementation and validation map,
  precise useful-work growth objective, and #960 closeout reconciliation.
- `superlinear/README.md`: original additional audit, with file/symbol locations,
  algorithms, measured controls and original limitations. Its statements that no
  issues were changed describe the original audit, not this later publication.
- `superlinear/probe_quadratic.py`: exact source-extracted algorithm/SQL probes.
- `superlinear/probe_query_plan.py`: experimental reachability join-order control;
  it reduces cubic behavior to quadratic on the audited SQLite plan, **not linear**.
- `superlinear/test_probes.py`: nine evidence/output/source checks; these are not
  product regression coverage or full wire-contract tests.
- `superlinear/results.json`, `reachability_join_order.json`: preserved
  original measurements. Source files have exact recorded hashes. Seconds are
  host-specific; operation counts are the useful evidence.
- Expanded candidate workload/counter dimensions in `WORKLOADS.json`.
- `HANDOFF.v1.md`: original handoff provenance, retained without rewriting it.

The original `RESEARCH.md`, `probe_scaling.py`, `test_probe_scaling.py`, and
`SOURCE_PROBE.json` remain unchanged as historical research. This revised handoff
and the newer owning-issue decisions supersede their earlier sequencing.
None of these reference-only inventories should be copied to main as another
policy ledger. Production behavior and regressions belong with their current
owners; decisions and dispositions belong in GitHub.

## Validation actually performed for this revision

- Re-read current main and #963/#964 decisions, #960's latest reported checkpoints,
  and the published reference tip. Main still names the audited base above.
- Verified the supplied audited-base clone ZIP SHA-256
  `0703fe0cbfd2201e8fb0866a271e723a22a43a2b2a99740b28956e3c461df233`
  and Git commit/tree. Restored ZIP-lost executable bits from Git before comparison.
- Reconstructed the prior six-file reference tree and verified exact equality to
  published tree `aafd4d0c0a9e7910f1b6c40435c7d832ba522f75`.
- Original audit ZIP SHA-256:
  `5abf3606b0752883877142df47088a55c11ed36d4381ee613b9de70b4c95e30e`.
  All seven recorded source hashes match both current bytes and the audited Git
  blobs. The eight findings use these files; hash count is not finding count.
- Re-ran all twelve probe/control cases at 128/256/512 and the join-order control
  on Python 3.13.5 / SQLite 3.46.1. All non-time counters, output counts, source
  identities and query plans reproduced exactly. Host elapsed values varied.
- All nine additional evidence checks and all four original research tests passed.
  Retained original measurements were not replaced by rerun timings.
- Final publication verification checks reference-only changed paths and exact
  local/published tree identity; the issue publication comment records the outcome.

## Not validated / limitations

No runtime fix is implemented by this revision. No locked full lint/unit/dist/build,
Docker/PostgreSQL scale lifecycle, final-image recovery, CI/CodeQL, or 10k/100k
qualification was executed. Source-extracted probes use controlled internal records;
several wire/model boundaries are intentionally outside their scope. Read their
README before treating them as anything more than algorithm witnesses.

The cubic measurement depends on SQLite 3.46.1's query plan in the fresh scratch
schema. Repeated reachability itself remains quadratic with the tested better plan.
Dynamic graph validation must preserve every required accepted snapshot; this
handoff does not claim a universally linear online dynamic-graph algorithm.

Not all eight findings dominate the 128-WAV proof: its one-sidecar fixture hides
balanced pairing growth; independent recovery is a separate path. They demonstrate
why a small successful timing alone is insufficient for 100,000-member support.
There is no established current full-lifecycle lower bound or sub-ten-minute result.
The #960 recommendation is an engineering disposition, not a mathematical proof
that 600 seconds is impossible before all #964 changes.

The available publisher exposes no native GitHub Development/dependency mutation.
The #963 prerequisite and reference relationship remain textually recorded; a later
capable publisher may establish native links without rewriting the published SHA.
No main/release source, protection, issue body or issue state is changed here.

## Authority

Authority remains with #964, the maintainer's issue comments, applicable component
contracts, and the repository's normal integration rail. #948/#953 retain separate
execution/recipe authority. Reuse is an explicit integration act with fresh tests,
not a mechanical cherry-pick of `.reference` or unreviewed change to commitments.
