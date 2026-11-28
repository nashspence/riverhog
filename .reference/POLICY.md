# Account for real work, then adopt its budget

This is the first design task for [#1006](https://github.com/nashspence/riverhog/issues/1006). Use the existing product contracts and source owners. A short issue comment is sufficient; do not add a permanent cost registry or runtime accounting system.

## One useful work account

For each significant repeated step on one complete publication path, record:

| Field | Required question |
| --- | --- |
| Owner and source | Which actual function/component causes the work? |
| Product promise | What user result, failure guarantee or current authority requires it? What counterexample breaks if it is removed? |
| Classification | Required core, optional progress/resumability/logging, avoidable implementation work, or unresolved hypothesis? |
| Extent and frequency | Per invocation, member, byte, history unit, poll, transaction or periodic event? |
| Evidence | Actual wall/CPU/call/transaction evidence and its limits; distinguish a source hypothesis from a measured bottleneck. |
| Decision | Keep, simplify, suppress at origin, remove, or one next measurement. Name the affected proof. |

**Core is a configuration, not a certificate of necessity or optimality.** All-optional-off can still repeat scans, imports, serialization, SQL, synchronization or polling unnecessarily. Do not protect accidental work by assigning it to C, and do not give optional features more allowance simply to make today's implementation green.

Preserve actual product outcomes: current authorization; correct archive/history identities; encryption and independent recovery; acknowledged results and ambiguous external effects; bounded work; original resource receiving. Existing mechanisms must justify how they establish those outcomes. A scheduler yield does not itself require a durable checkpoint. A named checkpoint does not automatically become optional.

## Source-backed first seams

All Riverhog links below are pinned to audited main `be483fd6d567616d5fcd355202d61f1e682efe75`.

| Owner | What to investigate without presuming a conclusion |
| --- | --- |
| [CollectionProducer / IncrementalCollectionProducer](https://github.com/nashspence/riverhog/blob/be483fd6d567616d5fcd355202d61f1e682efe75/packages/riverhog-client/src/riverhog_client/producer.py) | Public publish, finish and custody drain; source preparation/hashing, actual upload concurrency and polling. Public polling defaults to 2.0s with a 0.05s floor. Measure actual waits before changing completion/control design. |
| [ValidationLedger](https://github.com/nashspence/riverhog/blob/be483fd6d567616d5fcd355202d61f1e682efe75/packages/riverhog-provenance/src/riverhog_provenance/_session_checkpoint.py) | Parent checkpoint load/decode/compare/update per semantic step, owner epoch/currentness, and whether duplicate representations can be removed while preserving the actual fence. The unsafe attachment bypass is not that proof. |
| [JournalStore](https://github.com/nashspence/riverhog/blob/be483fd6d567616d5fcd355202d61f1e682efe75/packages/riverhog-provenance/src/riverhog_provenance/journal_store.py) | Retained versus disposable state, transaction granularity and actual synchronization promises. Main already distinguishes FULL for retained and OFF for non-retained creation; do not blindly force retained state OFF. |
| [Publication diagnostic fixture](https://github.com/nashspence/riverhog/blob/be483fd6d567616d5fcd355202d61f1e682efe75/tests/support/qualification/publication_profile.py) and [history helper](https://github.com/nashspence/riverhog/blob/be483fd6d567616d5fcd355202d61f1e682efe75/tests/support/qualification/canonical_history_scale.py) | Useful attribution/semantic checks, with in-process HTTP, in-memory ciphertext, one-member append and faster fixture polling. These do not supply real-backend or actual parallel-upload acceptance. Separate fixture orchestration from product work. |
| [Filesystem recovery qualification](https://github.com/nashspence/riverhog/blob/be483fd6d567616d5fcd355202d61f1e682efe75/scripts/test_filesystem_recovery_qualification.sh) | Reusable real client/server/Postgres/filesystem topology and recovery checks. Its KDF10 (runtime default18), tmpfs client state, ordinary Docker volume and deployment-local HTTP are test settings to reconcile deliberately, not silently inherit. |
| [Existing profile/evidence owners](https://github.com/nashspence/riverhog/blob/be483fd6d567616d5fcd355202d61f1e682efe75/scripts/performance_measurement.py) | Preserve make profile and matched diagnostic evidence. Shared absolute acceptance is separate; do not turn a historical ratio comparator into a budget gate. |

Choose the dominant measured phase first. Retain immutable-value results only within a justified lifetime and preserve fresh authorization. Consolidate transactions only at a boundary compatible with acknowledgments, interruption, cleanup and growth. Count reduction alone does not establish latency improvement.

## Core and optional work

[all#68](https://github.com/nashspence/all/issues/68) already implements schema-2 complete-configuration envelopes. [all#67](https://github.com/nashspence/all/issues/67) describes the proposed controls and work-origin suppression; the shared runner does not implement them for Riverhog.

- **Core C:** the complete real operation with applicable optional work off. Required durability, identity, cancellation, errors, result/receipt and custody remain.
- **Progress P:** optional status/report production. Required scheduling/heartbeat/currentness is not made optional merely because it reports state.
- **Resumability D:** optional derived state that avoids otherwise safe recomputation. A durable acknowledgment or authoritative receipt is not optional acceleration. Do not create an absent feature to satisfy the table.
- **Logging L:** optional diagnostics, including eager formatting, serialization, queueing and flushing. Required result/errors and genuinely required audit/effect evidence remain core.

Use one effective invocation/workflow selection in existing configuration. Propagate it through owned descendants; child defaults cannot re-enable disabled work. Suppress producers before their optional setup, not just sinks. Avoid a second executor, duplicate config vocabulary or generic orchestration layer.

Declare all three applicability choices explicitly. Null means inapplicable; zero means applicable without extra headroom. A fixture that misses an existing periodic checkpoint has not proved inapplicability. Choose an actual periodic witness when that claim becomes necessary.

The envelopes C, C+P, C+D, C+L and C+sum(applicable allowances) constrain full configurations. They do not establish independent feature-cost caps; unused core headroom and interactions remain. A genuinely separately attributable production unit may get its own ordinary absolute cap. Do not create an artificial decomposition merely to populate labels.

## Small design decisions, not another study

The first issue checkpoint should select the real p1 warm/cold operation, supported configuration/storage/history extent, applicable features and actual completion promise. It should name C/P/D/L values with operator-usefulness rationale for independent approval; no candidate or measurement result approves itself. The user-supplied marginal bands guide that decision, but a fixed-cost term and real full boundary are still necessary.

The prior six-row p1/p2/p4 first-adoption proposal is superseded by an explicitly scoped p1 pilot. Keep unmeasured p2/p4/periodic/recovery scope in #991; do not claim it passed or silently waive an already accepted requirement. Do not hold useful accounting/diagnostics/simplification while those acceptance decisions are made.

## Policy source and enforcement

The [proposed guide/test patch](proposed/agents-policy.patch) replaces the blanket advisory-only production policy, covers growth/evidence/handoff clauses, updates the existing guide hash and retains the 2,100-word cap. It is a proposed source change, not an installed gate.

Integrate the actual evidence path through the existing owners:

| Owner | Change required |
| --- | --- |
| AGENTS.md; tests/unit/test_entrypoints.py | Apply/adapt the intentional policy amendment and hash/wording tests. |
| scripts/transfer_profile.py; scripts/performance_measurement.py; Makefile | Reuse existing diagnostic entrypoints; add thin real-operation bindings rather than another runner. Keep matched comparisons diagnostic. |
| scripts/ci_qualification.py; scripts/ci_actions.py; scripts/workflow_evidence.py; validation_evidence.py | Consume the exact-candidate shared verdict and complete evidence. Missing, failed, unqualified, skipped or stale required work cannot become acceptance success. Ordinary suite targets remain advisory. |
| .github/workflows/ci.yml; validation-evidence.yml; release.toml | Wire the trusted dispatch/result into the actual campaign/release acceptance owners. Current development governance has no PR/pre-push status requirement; a merge-required rule is an explicit additional governance change. |
| scripts/qualification_source.py and build owners | Shared source admission also rejects ignored files. Keep dependency/build/work/output roots external and bind actual installed artifacts to candidate source. |

PR #1001's unmerged controller/configuration/schema/state additions are preserved only as optional evidence/source. This transition adopts none. They need no migration or cleanup from main simply to retire them. Selected future changes incur only their actual product-contract/state/install obligations.

## Cross-repository feedback

All owns the small general declaration, clock, profile approval/admission, activity-witness shape and verdict. Riverhog owns meaningful workloads, necessary-work decisions, controls, numbers and production evidence. Do not copy the shared compiler, mounting helpers, profile registry or acceptance logic into Riverhog.

For a reproduced shared usability gap, record one failing real consumer case, the generic requirement it exposes, the smallest shared change and why it still fits Fishhat. Make that reviewed change at the shared owner; material runner/profile changes require applicable shared qualification. Do not assume today's implementation is immutable, nor start a speculative abstraction redesign.

Fishhat's [workflow journal](https://github.com/nashspence/fishhat/blob/f96b83dc20f059d62a4363a63c37bb0ef8c642e5/fishhat/owner-support/src/fishhat_owner/workflow_journal.py) and [checkpoint store](https://github.com/nashspence/fishhat/blob/f96b83dc20f059d62a4363a63c37bb0ef8c642e5/fishhat/owner-support/src/fishhat_owner/checkpoint_store.py) demonstrate the distinction: protected acceptance/continuity commits and actual held CAS locks remain mandatory. Its later adoption needs its own scope/values; no artificial parallel variant is required for a serial operation.
