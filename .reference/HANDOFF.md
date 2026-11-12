# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue: [#989](https://github.com/nashspence/riverhog/issues/989)
Convention: [#903](https://github.com/nashspence/riverhog/issues/903)
Branch: `reference/external/989-asynchronous-retirement-growth`
Audited base: `codex/964-session-owner@dd09f4ab5e824935e680f26fba3d73d2484cb426`

Produced by: OpenAI ChatGPT
Model: GPT-6 Astra Pro
No additional configuration/effort identity is attested.
Requested by: Riverhog maintainer, for the integration agent
Published through: the connected GitHub integration

## Start here

Read [ASYNC_DESIGN.md](ASYNC_DESIGN.md). The selected contract separates
coordinator bookkeeping, exact original worker execution and resource receiving.
For positive limit L the scheduling return is bounded by 4L, not L confirmed
file progress: L completions + L ordinary withdrawals + L close resubmissions +
L new submissions. This is derived from source phases, not fitted to a sample.

**First useful slice:** reproduce the actual 17-byte/three-byte-budget 1-then-3
return on the unchanged base. Apply the explicit candidate in a disposable
worktree and verify original resource receiving, actual join and still-charged
ordinary retention separately. The four-event and repeated-close vectors then
exercise the complete envelope without inline file work or replacement actors.

[QUALIFICATION.md](QUALIFICATION.md) gives the commands. `prepare.py` is the single
candidate assembly entrypoint, using the exact-base `apply.py` helper and the
supplied source/test modules. The published product tree is unchanged. The
maintained framework validates actual child samples; the test-only transcript
checker separately rejects missing/cross-original causal links. Exception-frame
receiving is placed on original execution, not hidden in scheduling collection.

## Source, evidence and revision relationship

This initial #989 packet is append-only. `4c920e9076c10086cdc8b9056601e619bceb6f05`
contained the first executable candidate; its observer failures remain recorded.
`b4f4d25a558a0a90a0709a42fc841b12bdfbaa35` corrected those observers and passed
32 maintained-owner tests on Linux and macOS. Artifact inspection then identified
an incorrect diagnostic retention-key lookup; the current candidate corrects
that observation and adds a finite transcript validator/negative controls.
The original receiving assertions were independent and passed. No product
algorithm, budget, regime or growth bound was loosened for that correction.

[VALIDATION.md](VALIDATION.md) separates those runs, source identities and limits.
The exact current commit's later workflow outcome is recorded in the owning
issue; ancestor success is not asserted as a pass for a changed observer.
[RESEARCH.md](RESEARCH.md) links pinned owners and primary Python documentation.

The reference adds `.reference/` material and one branch-scoped Linux/macOS
workflow. Product paths, existing workflows, dependency locks and required gates
remain equal to the audited base. No main/integration branch is changed. This
does not revise #987/#988 resource authority or activate normal runtime work.

## Authority

This is non-authoritative external input. Authority remains with #989, current
maintainer decisions and the normal integration rail. Reconcile current source,
explicitly consume the candidate, and record finite adoption/reproduction or
rejection with actual integrated evidence. Do not mechanically merge this branch.
The exact SHA linked in #989 is the handoff identity; the branch is navigation.
No native Development association is requested or expected. Full final-source/
tested-merge product gates and #964's independent-arm 1,000-member and retained
128-file lifecycle campaigns remain separate work.
