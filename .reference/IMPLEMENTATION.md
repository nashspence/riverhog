# Proposed aggregate durability qualification

Owner: [#1000](https://github.com/nashspence/riverhog/issues/1000). Controlling design: [#994](https://github.com/nashspence/riverhog/issues/994), including its [starting-order decision](https://github.com/nashspence/riverhog/issues/994#issuecomment-6921730895). External reference convention: [#903](https://github.com/nashspence/riverhog/issues/903).

This proposal settles the qualification implementation, evidence boundaries, first slice, state/installation checks and stopping rule. It does not implement the underlying durability redesign or claim aggregate closure. Product files in this branch match **codex/964-session-owner@c28eceaa2dfec1b3712135469ba12230e74f5ffa** exactly. The only additions are under `.reference/`. Reconcile then-current [#979](https://github.com/nashspence/riverhog/pull/979) before integrating; preserve its WIP.

## 1. Begin with the real publication slice

Start qualification alongside #995/#996/#997 on the existing authenticated/encrypted **F1/C1/K2/Q1 collection-publication** scenario. Integrate the common root budget, live Native execution and final/generation-only Core adoption together. Preserve required intent, authored observations, exact transport/crypto state, receipts, authorization and original resource receiving. The cheap-work acceptance is zero optional checkpoint creation **and zero optional resume-store initialization**, with complete publication and independent fresh/warm/evicted verification.

Do not postpone this slice for a whole-repository census or all of #964. Run source-derived discovery incrementally and finish its reconciliation before parent closure. After cheap work succeeds, use the same executor to demonstrate actual adaptive promotion, interruption and restart. A separate fast path plus the old durable microstep executor does not satisfy the cutover.

The reference supplies a newly executed current-source F1 observation, an executable three-arm comparison proposal, a source-derived coverage probe, and actual Native process-kill witnesses. Its current F1 is the preserved implementation only. No redesigned restart/adaptive pair exists at the audited product source; reference arithmetic and simulated comparison rows do not supply that missing evidence.

## 2. Reading map and implementation ownership

| Detail | Current reference |
|---|---|
| Actual discovery, component/state/configuration coverage and final platform gates | [COVERAGE_AND_CUTOVER.md](COVERAGE_AND_CUTOVER.md) |
| Paired observations, completion clocks, costs and comparison implementation | [MEASUREMENTS.md](MEASUREMENTS.md) |
| Changed failure boundaries, actual process-kill witnesses, lifecycle growth | [SAFETY_AND_GROWTH.md](SAFETY_AND_GROWTH.md) |
| Exact policy test obligations and finite parent closure | [POLICY_AND_CLOSURE.md](POLICY_AND_CLOSURE.md) |
| Executions actually performed and explicit limitations | [VALIDATION.md](VALIDATION.md) |

The following are separately pinned inputs, not production dependencies or additional authorities. Their results retain their original scopes and must not be counted again as independent #1000 proof.

| Owner | Pinned input | Responsibility retained by that owner |
|---|---|---|
| #995 | [b545a77996fbc9fe98019796860473bda1bb1aa0](https://github.com/nashspence/riverhog/blob/b545a77996fbc9fe98019796860473bda1bb1aa0/.reference/HANDOFF.md) | Shared predicate, configuration, root useful credit, reservations and conservative restart accounting |
| #996 | [e4a607081d17ae1a067bb766241bd5febd9ddeb9](https://github.com/nashspence/riverhog/blob/e4a607081d17ae1a067bb766241bd5febd9ddeb9/.reference/HANDOFF.md) | One Native semantic executor and independently usable optional generations |
| #997 | [a7ea19bf795c249476c9d767eb821d74bcf98b2a](https://github.com/nashspence/riverhog/blob/a7ea19bf795c249476c9d767eb821d74bcf98b2a/.reference/HANDOFF.md) | SDK/Core production, exact effects, publication, complete index acceptance and custody |
| #998 | [e2083a7a17f33820bca4ed696a97fa50f317cbcf](https://github.com/nashspence/riverhog/blob/e2083a7a17f33820bca4ed696a97fa50f317cbcf/.reference/HANDOFF.md) | Processing, Stove0, ingress, supplied applications and shared parent-driven work accounting |
| #999 | [25d97a3803eb924a629359637b02849023022496](https://github.com/nashspence/riverhog/blob/25d97a3803eb924a629359637b02849023022496/.reference/HANDOFF.md) | Storage/copy/retirement, retrieval/cache, discovery, local publication and independent recovery |
| #1000 | This reference | Incremental cross-owner qualification, matching measurements, coverage reconciliation and aggregate evidence closure |

#995's earlier product base is `0f8030b0c0cd53eb33456924f070ae5979ed307e`; #996–#999 use c28. The c28 intervening source change preserves Python witness bytes across native checkouts. Those reference trees have not been rebased into this branch or represented as implemented product behavior.

## 3. Concrete product changes for the integration agent

Keep new executable behavior with its existing owner. Reference modules are reviewed implementation input: move/adapt their functions and tests into these owners through normal integration, not a copied `.reference` subtree.

| Existing owner | Selected change | Validation |
|---|---|---|
| [scripts/performance_measurement.py][performance] | Add the strict, typed durability comparison record and paired arithmetic from [measurement_comparison.py](implementation/measurement_comparison.py). Require exact matching conditions, source/artifact identity for redesigned arms, original completion evidence, actual policy and explicit counter availability. Preserve signed differences and report-only timing. | Adopt the comparison tests alongside `test_performance_measurement.py`; malformed or forged required evidence is an error, while absent or valid-but-unmatched comparison arms yield structured `not-compared`. Retain adverse observations. |
| [scripts/transfer_profile.py][profile] | Extend the existing scenario path and reference ingestion with explicit arm/comparison identity; do not introduce another developer-facing profiling command or scheduler. Same scenario and executor produce B/C; the legacy arm is identified separately. | Extend `test_transfer_profile_script.py` and `test_publication_profile.py`, including failure/cleanup/changed-source paths. |
| [tests/support/qualification/publication_profile.py][publication] and [publication_evidence.py][publication-evidence] | Emit exact user-result and terminal-receiving milestones from the actual owners, with a declared clock domain and output correctness receipt. Keep fixture preparation, full verification, command envelope and observer work separately named. | Prove event order, missing milestone refusal, error paths and receipt persistence. Never reinterpret the current coarse timer as a finer boundary. |
| [scripts/ci_qualification.py][qualification], [validation_witnesses.py][witnesses], [ci_actions.py][actions] | Reuse current source-owned work/growth discovery, exact execution/attempt collection and artifact custody. Add necessary bindings where changed owner tests enter existing gates; preserve complete parametrized-case and teardown checks. | Existing source/attempt/platform/scope/duplicate/skip/refusal/cancellation consumer tests plus changed owner witnesses. No second qualification registry or parallel CI gate. |
| Source-owned Native, Core/SDK, application and storage tests | Replace obsolete private phase assertions with equivalent semantic/effect/generation counterexamples. Integrate the actual retained-session kill witness into the existing Native test owner. Keep process, OS/filesystem and provider fault models distinct. | The boundary matrix and growth/capacity requirements in [SAFETY_AND_GROWTH.md](SAFETY_AND_GROWTH.md). |
| Contract/configuration/state/installation owners | Update only changed public/configuration/current state surfaces and all actual consumers. Remove superseded runtime aliases/readers/writers; retain supported upgrade machinery and incompatible-state refusal. | Derived discovery/contract parity, state data-preservation, built/installed/native and backend gates in [COVERAGE_AND_CUTOVER.md](COVERAGE_AND_CUTOVER.md). |

Do not make the source-discovery probe a product runtime component. Its output is a generated research snapshot, not a new normative owner list. A new source owner discovered later is evaluated under the same existing authorities, not excluded because this snapshot predates it.

## 4. Evidence must prove the claim being made

The repository already separates validation results, timing observations and growth obligations. Keep those boundaries:

1. Derive executable definitions and test/operation/delegate bindings from the exact source. Record the effective numeric resource profile using [validation_numeric.py][numeric]; its reader identity and limits are part of the observation.
2. The actual owner records execution, output, source before/after, phases and receiving. An assessor must not rerun the work merely to reconstruct missing original evidence. Before/after clean checks assume exclusive checkout ownership; they do not detect an unobserved transient edit restored between checks.
3. Consume the original run/attempt/job/session/node identities and exact retained bytes through existing validators. The validator must reject a changed source, missing required case, expected failure, skipped setup/call/teardown, wrong platform, incomplete child, conflicting duplicate, changed declaration or missing numeric receipt.
4. Matching performance conditions permit a comparison; they do not establish archival correctness. A self-reported JSON flag cannot replace original process/backend/effect receiving. Syntax checks on synthetic rows qualify the comparator only.
5. Separate complete applicable test evidence from overall cutover completeness. A valid old-source fixture, local subset, generated contract, source declaration, Linux process-kill result, or successful reference suite has a bounded claim.

The existing [CI gate][ci] verifies mandatory job outcomes and exact-attempt evidence. Keep it. A required skipped/blocked job remains incomplete; an unchanged old success cannot replace a later failed attempt. Legitimately inherited success is reused only through the existing original-execution reconciliation, with the original source, job/attempt and artifacts preserved. Report fresh retry cost separately.

A required declared growth violation, invalid evidence or correctness failure fails qualification. A valid elapsed-time target miss is a reportable observation. Systematic optional-budget over-admission still requires the concrete estimator/backoff correction or explicit issue-local maintainer disposition required by #994/#1000; do not turn it into a flaky CI wall-clock assertion or silently change its denominator.

## 5. Close the shared seams, once

#1000 consumes each shared implementation once at its actual owner and traces the real callers through it. It does not create another budget controller, generation engine, custody service, scheduler, state database or provider policy.

The integrated slice must establish these relationships together:

- **One logical workflow root:** Native, SDK, Core, application and transport descendants cannot each receive a new 1/8 allowance. Saved receipts, repeated polls, replayed work, idle waits and renamed phases create no useful credit.
- **One semantic executor:** restart and adaptive differ in optional generation admission/publication, not semantic record validation or required effects.
- **Exact archival and mandatory authority:** missing optional state falls back safely; corrupt archival bytes, sole nondeterministic evidence, unknown transport/crypto state or unresolved custody remain errors/retained obligations.
- **Bounded control without optional persistence:** cancellation, lease renewal, status and independent jobs remain responsive while optional writes are disabled. Preserve safety-required lease, heartbeat and fencing state at its existing owner. No shared catalog writer encloses provider wait or unbounded CPU work.
- **Original receiving:** a timeout, lease expiry, cancel request or future state cannot release the original input, worker, provider ticket, reader, quota liability or workspace.
- **Complete lifecycle accounting:** construction, snapshot attempts, failed generations, resume validation, spill, compaction, late results and reclamation are charged to their real owners. Parent inclusive wall time is not added to overlapping child wall time.
- **Public activation and installations:** private helper tests cannot substitute for the real API/SDK/CLI/application/image route. Installed recovery must remain independent of Riverhog service/client/database/checkpoint stores.

The linked #995–#999 references spell out these mechanisms. If current accepted issue decisions supersede an input, use those decisions and record the finite disposition on the owning issue. Do not open a competing cross-repository design program.

## 6. Qualification sequence and finite stop

**First slice:** exact default predicate/configuration plus cheap Native → SDK/Core F1, complete encrypted archive verification and mandatory failure/receiving cases. Capture legacy A before removing the old executor; build redesigned B/C from the same candidate source. Begin discovery concurrently.

**Promotion slice:** actual supported long/unknown-length work crosses eligibility on the same executor, selects a usable generation, suffers controlled interruption and resumes with measured saved work/validation and no duplicate paid effects. Reduced-threshold deterministic tests are useful boundary tests, but cannot be reported as the default two-minute product demonstration.

**Family slices:** add #998/#999 consumers as their shared contracts become usable. Cover independent dimensions and changed public/state/installed paths. Reuse shared primitive proofs at their declared scope, and execute the distinct composed workflows. The held 1,000/10,000/100,000 campaigns are not made prerequisites or launched by this proposal.

**Final candidate:** reconcile the source-derived coverage against current issue dispositions; remove superseded production paths; generate contracts and state baselines from the candidate; execute applicable existing local/backend/native/installed/CI gates on the exact final head and tested merge source. Reconcile any later source or dependency change before reusing evidence.

**Closure:** use [POLICY_AND_CLOSURE.md](POLICY_AND_CLOSURE.md) to record the claim-to-evidence decision on #994. Keep #991 paused until that parent completion or explicit maintainer reprioritization. Finish the finite cutover; unrelated #964/#469 work, release/provider permissions and publication remain with their owners.

[performance]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/scripts/performance_measurement.py
[profile]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/scripts/transfer_profile.py
[publication]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/tests/support/qualification/publication_profile.py
[publication-evidence]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/tests/support/qualification/publication_evidence.py
[qualification]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/scripts/ci_qualification.py
[witnesses]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/scripts/validation_witnesses.py
[actions]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/scripts/ci_actions.py
[numeric]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/scripts/validation_numeric.py
[ci]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/.github/workflows/ci.yml
