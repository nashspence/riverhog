# Developer guide

Read [README.md](README.md) for Riverhog’s introduction.

The README explains Riverhog’s durable system boundaries. This guide explains how to work inside them, for human contributors and automated agents alike. It states repository-wide work rules and points to the sources that own detailed behavior.

## Start from the source that owns the answer

Before editing, read the relevant implementation, public contracts, and scoped policy tests. Enforceable repository policy belongs in clearly named tests.

Keep hand-maintained documentation focused on context that cannot be recovered easily from executable sources. Release reference belongs with the release it describes, not as a separate reference manual on `main`. [Licensing](LICENSE.md) and [security reporting](SECURITY.md) keep their own roles.

Generated reports and audit findings describe declared contracts; they do not create new public promises. Do not introduce a separate registry to repeat rules already owned by contracts or tests.

Change `README.md` or `AGENTS.md` only to correct material drift or when the maintainer explicitly directs broader work.

In short, **explain lasting context here; keep exact behavior with the source that owns it.**

## Keep work tracking outside the repository

Repository files describe the current system, not the work queue or the history of retired designs.

Do not store work-tracking information in source, tests, fixtures, configuration, or documentation. That includes TODO or FIXME reminders, progress checklists, future-work plans, issue or pull-request references, and descriptions of obsolete architecture. Explain current design reasons where useful, not the history of getting there.

GitHub issue bodies own scope, decisions, and acceptance. Preserve existing bodies; record later decisions in comments. Use native sub-issues, dependencies, and milestones rather than copying their state into parent checklists or title prefixes. Checklists cover only acceptance steps owned by that issue.

Additional work after completion belongs in a linked follow-up issue, not a reopened or rewritten completed issue.

In short, **the repository describes the current system; GitHub records the work that changes it.**

## Put each change with its owner

Products, applications, and components integrate through public contracts, not imports of one another’s implementation code. Reusable packages must not import those implementations. Put genuinely shared behavior in a focused package.

Keep core behavior separate from how requests arrive, results are presented, and outside systems are called. Applications own their workflows and state. Components selected independently retain separate identities and authority even when they share packaging.

Keep interpretation with its owner, too. Generic code should not acquire file-format or provider-specific rules that belong to a component. Public code stays generic; provider selection, private deployment identity, and deployment layout belong with their owners.

The [workspace boundary tests](tests/unit/test_workspace_boundaries.py) define the precise ownership and dependency rules.

In short, **change the owning implementation and cross its boundaries through contracts, not borrowed code or authority.**

## Preserve custody and independent recovery

Archive stores hold archive authority; databases, caches, and workflows do not replace it. Preserve archived bytes, recorded provenance, and the information required to verify and recover them together. Coordinate catalog and stored-object changes as one archive operation.

Act only on the exact inputs and within the current permissions granted to the operation. A known identity or an earlier authorization does not replace the checks required by its contract.

Incomplete or unverifiable work is not successful preservation. Keep recorded history intact across handoffs; permitted omissions require an explicit reason. The [archive contracts](packages/riverhog-archive-contracts/) and [provenance contracts](packages/riverhog-provenance-contracts/) define the exact requirements.

Preserve confirmed outcomes across retries. A timeout does not prove that work failed, and a cancellation request does not prove execution has stopped. Release data or execution resources only when the owning contract says it is safe.

Use authenticated, encrypted connections for incoming plaintext. An unencrypted connection requires explicit operator opt-in on a trusted local network reserved for one operator’s deployment. Protected archive and retrieval-cache objects remain encrypted outside Riverhog processes.

Keep the [independent recovery tool](some-implementations/riverhog/recovery/) independent of the server, client, and database. Never expose secrets or private deployment identity in public code, fixtures, logs, examples, or generated contracts.

In short, **preserve complete, authorized, verifiable custody without adding plaintext exposure or recovery dependencies.**

## Bound the work, not the archive

Large collections should not require processing everything at once. *Bounded work* handles a limited portion at a time; a page of results does not limit the total number of results. Long operations progress through bounded steps that can resume after interruption.

Do not introduce a total limit unless the owning contract declares it. Resource limits may explicitly reject, delay, or slow work, but must not silently omit data. Limits that depend on the deployment and affect accepted work belong in operator configuration.

The [*extent contract*](scripts/extent_contract.py) owns these rules about sizes, counts, and capacity.

In short, **limit each step without inventing hidden limits or reporting partial work as complete.**

## Preserve declared growth bounds

Limiting the size of each step does not guarantee that the total work stays manageable.

A *growth bound* describes how an operation’s work increases as its input grows. For ordinary data transfer and collection traversal, require *linear growth*: work proportional to the bytes, members, and necessary metadata processed. Different costs must be declared and justified by the operation’s owner.

Reason about these bounds and test them with the maintained implementation. Include difficult inputs, resource limits being reached, interruptions, and retries. Do not hide repeated whole-resource work behind batching, concurrency, or caches.

Violating a declared growth bound is a validation failure. Machine-dependent timing targets remain performance observations, not product guarantees. Neither permits incomplete results, weaker custody or authorization, or hidden total-size limits.

In short, **bound total work as well as each step, and test the bound rather than trusting a small successful run.**

## Keep coordination responsive

*Coordination* schedules work, checks progress, and requests cancellation. It should remain responsive while the work itself runs.

Keep scheduling and status calls bounded. Long-running execution and waits for outside resources stay with the component doing that work, not with the code coordinating other jobs. A waiting job must not prevent independent jobs from progressing.

Progress reporting must not block data transfer. Reduce the cost of checking progress without removing required ownership or integrity checks. The [control-call contracts](packages/http-api-contracts/src/http_api_contracts/control.py) and [scheduler tests](some-implementations/stove0/application/tests/test_scheduler_roles.py) define the detailed boundaries.

In short, **slow work and progress reporting must not stall unrelated work.**

## Make clean pre-v1 changes

Before the first v1 release, make *hard cuts*: replace the previous model completely rather than supporting old and new forms together. For example, a renamed field should not leave its old spelling accepted.

Update affected code, contracts, tests, examples, documentation, and dependent components together. Do not preserve obsolete interfaces, aliases, old readers or writers, parallel formats, or upgrade paths for abandoned pre-v1 designs. If a dependency cannot change with the rest, report the blocker rather than retaining the obsolete design.

A hard cut does not permit deleting durable user data or changing protected commitments. Review contract changes explicitly. Regenerating contracts or updating expected test results does not authorize changing a [protected boundary](tests/unit/test_contract_freeze.py). Keep [maintained interfaces consistent](tests/unit/test_public_interface_parity.py) with the same operation.

In short, **replace obsolete pre-v1 models completely without discarding durable state or bypassing protected boundaries.**

## Change durable state deliberately

An application database may contain information that can be recreated from the archive and information that cannot. Being able to rebuild some of it does not make the whole database disposable.

Each durable database belongs to one application. Before the first v1 release candidate, changes to its stored structure replace the previous pre-v1 version completely. Update the related code, contracts, and tests together rather than preserving support for abandoned versions.

Keep the existing process and tests for upgrading databases between supported releases. Those tests must prove that existing data survives and interrupted upgrades can be handled safely.

New databases are initialized through that explicit upgrade process. Application startup checks that its database has the expected structure; it does not silently upgrade or erase existing data.

Recreate stored information only when it can be recovered from verified archive authority. If existing information cannot safely be recovered, its owner or operator must decide how to proceed. The [state-schema package](packages/state-schema/) owns the precise behavior.

In short, **replace obsolete pre-v1 database structures without sacrificing durable data or support for future upgrades.**

## Measure performance without creating gates

A performance target is a nonbinding goal for measured speed or elapsed time. Unlike a declared growth bound, it is not a contract. Missing a target does not fail validation.

Use `make profile` as the shared entry point for maintained profiling scenarios. Add new scenarios there rather than creating separate developer-facing commands. Their implementations remain with their owners.

Report verified work, elapsed time, actual rates, and measurement conditions. Compare only against matching measured references. An unavailable comparison is neither a pass nor a failure. Invalid evidence and failed execution remain errors.

The [measurement rules](scripts/performance_measurement.py) and [profiling tests](tests/unit/test_transfer_profile_script.py) define the precise behavior.

In short, **growth bounds are enforced contracts; performance targets are observations, never gates.**

## Account for validation evidence

Checks record their results and timing as they run, without repeating work just to measure it. The [shared qualification definitions](scripts/ci_qualification.py) identify the work and its conditions without replacing contracts owned by individual operations.

Keep correctness, growth-contract conformance, and elapsed-time observations separate. Correctness failures, growth violations, and invalid required evidence fail validation. A valid timing miss does not.

Report elapsed time even when no numerical target or comparable reference exists. Use credible performance evidence to recommend investigation or improvement. The stronger the evidence and the more serious its likely consequences, the stronger the recommendation may be.

Record significant findings and recommendations in GitHub. Performance targets do not create approval-to-proceed conditions or authorize weakening required tests, workloads, or safety limits.

In short, **validation evidence establishes compliance; performance observations inform judgment.**

## Validate the change you actually deliver

Use the versions of tools and libraries fixed by the repository rather than relying on whatever happens to be installed locally. Follow the [dependency](tests/unit/test_dependency_policy.py) and [image-build policies](tests/unit/test_image_build_graph.py); ship only the runtime content the component needs.

Run focused tests while working. Cover relevant failure, interruption, and retry paths as well as success. New components must enter the existing validation gates, not sit outside them. Check built and installed software where packaging, platform behavior, or handoffs are affected. A declared operation is not evidence that it ran correctly.

Before pushing directly to `main`, run:

```bash
make lint
make unit
make dist-smoke
make build
```

For integration work on a pull-request branch, run `make lint` and focused tests of the changed owners before checkpoint pushes. Use the pull request’s required CI suites for complete qualification. Work may continue while CI runs; inspect failures promptly and cancel superseded runs as needed. Before merging or handing work back, require complete passing checks for the exact final source and its tested merge checkout.

When exhaustive local qualification is needed, run `make linux-qualification` instead of those four separate commands. This path includes the complete unit shards, lint, distribution smoke tests, image qualification, and Linux lifecycle checks. Use `make help` for current command guidance.

After pushing, watch that exact commit’s required GitHub Actions checks through completion. Fix failures before handing work back. A skipped or blocked check is not a pass; report the gap rather than claiming complete validation. Do not remove required coverage or weaken a check merely to make a result green.

Include correctness results, required growth evidence, and timing observations in the handoff. Record significant performance concerns as evidence-backed recommendations, not validation failures or approval conditions.

In short, **validate the actual change through every required gate, not a nearby version or a convenient subset.**

## Keep authorization separate from validation

[Release governance](release.toml) and the [release workflows](.github/workflows/) define branch, qualification, and publication requirements. Main remains the pre-v1 development authority, with required checks for delivered changes. Maintainers may commit directly or use pull requests to validate integration branches before merging.

Updating the release branch requires explicit approval of a fully green commit. This does not select a *release candidate*, a version chosen for release testing.

Provider qualification remains disabled until separately authorized. Publication requires protected approval and a complete governance check, not just the narrower checks available during routine validation. Published v1 tags are never moved or deleted.

In short, **successful validation supplies evidence, not permission to synchronize, qualify, or publish.**
