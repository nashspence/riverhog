# Enforced contracts; advisory performance observations

The [#963 decision 6125400031](https://github.com/nashspence/riverhog/issues/963#issuecomment-6125400031) and maintainer-approved AGENTS wording supersede earlier
approved-target / approved-no-target / unresolved policies and target-shortfall
permission rules. This file explains the corrected implementation boundary.

## Authorities and consequences

| Input or outcome | Meaning | Effect on validation/handoff |
|---|---|---|
| Correctness failure | Owned executable requirement violated | Fails |
| Declared growth-bound violation | Operation contract violated | Fails |
| Independently declared safety/resource failure | That contract violated | Fails under that owner |
| Required evidence missing/invalid | Cannot establish the actual required work | Fails |
| Performance goal missed, even severely | Advisory observation | No failure, hold or permission condition |
| No numerical goal | Ordinary measurable work | No approval needed; report elapsed time |
| Comparison unavailable or conditions do not match | Neutral not-compared observation | Neither pass nor failure |
| Supplied observation fabricated or malformed | Invalid data, not a speed miss | Evidence/reporting error |
| Incomplete/interrupted execution | Required work not completed | Never a completed correctness/growth pass |

Requiring trustworthy elapsed evidence does not require a goal or a comparison.
Do not define an optional comparison as required evidence to recreate a speed gate.
The seriousness of a credible concern controls recommendation strength, never its
authority. No missed-target waiver, required investigation or resolution, or
maintainer approval to proceed is required. Significant findings/recommendations
belong in GitHub; recording them does not create an acceptance obligation.

## Executable split in the replacement patch

`scripts/ci_qualification.py` remains the shared home of executable work names,
commands, boundaries and conditions. Internal `scripts/validation_evidence.py`
contains typed result handling, not another registry or public growth authority.
`ValidationWork.definition()` excludes the optional target. There is no target
approval flag, target-presence requirement or unresolved-timing validation state.
`require_validation_work` checks actual work identity/coverage, not speed goals.

The run record's optional `advisory_target` is separate from `definition`.
Changing or removing the current goal cannot invalidate an earlier valid run.
When present, the recorded goal's number and comparison conditions must be truthful;
when absent, timing is still reported as not-compared. A supplied comparison is
checked against its recorded measurement/goal, never treated as a compliance result.
A matching-condition goal comparison is not a manufactured measured-reference rate.
Existing make profile measured-reference logic remains with its existing owner.

The runner executes the work once. Advisory overrun notification recommends
investigation without interrupting it. `check-evidence` now returns nonzero for
actual command/growth failures instead of confusing complete failure evidence with
success. Its `qualification_exit_status` has no data dependency on met/missed/
not-compared, target seconds, severity or approval. Tests cross these axes.

Growth outcome IDs must match the expected work's `growth_witnesses`. Empty
bindings do not certify absence of product growth obligations. The injected owner
collector is an integration seam for verified real artifacts, not arbitrary
self-reported pass strings. The patch tests coverage/result propagation; integration
must implement source/condition/declared-witness discovery and verify the actual
artifacts before calling this seam. False not-applicable cannot hide a required
bound. #964 supplies the actual operation declarations and counter assertions.

## Preserve these distinctions during integration

The replacement source patch targets 152d0950 and does not depend on the retired
patch chain. The guide patch instead targets #961's approved root AGENTS text.
Do not restore old approvals from historical comments, tests, documentation or
#964's earlier target/no-target routing. Where those references concern timing
only, this correction governs; growth/extent/security/release authority is unchanged.

Every maintained check still has known work, automatic trustworthy elapsed data,
complete execution outcomes, bounded measurement overhead and source/workload
conditions. Match measured references only when legitimately comparable. Record
preparation, execution, cleanup and actual parallel aggregate wall time without
rerunning work just to measure it. Retain failed/interrupted attempts honestly.
No mandatory numeric target, minimum comparative-run count, target approval status
or target-miss disposition is needed to begin, validate, hand off or complete work.
Representative evidence can justify strong recommendations for tuning a goal.

The maintained 600-second scale goal and its workload are unchanged and advisory.
Earlier permission to close despite a miss is historical, not a standing waiver
mechanism. A miss neither establishes compliance nor prevents it. Targets cannot
authorize weakening tests, shrinking workloads, hiding phase costs, discarding
history, raising safety deadlines, or rewriting independently owned resource limits.

Keep #961 -> #963 foundation -> dependent #964 integration. Complete real local/CI/
phase/aggregate adapters and the operation witness-consumption path, then normal
source-bound checks. These are implementation/evidence requirements, not a request
to attain any speed target. Normal independent release/provider permissions remain.
