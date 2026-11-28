# One integration slice

## Starting source

The request names `be483fd6d567616d5fcd355202d61f1e682efe75`. Current inspected PR [#1008](https://github.com/nashspence/riverhog/pull/1008) is `382870b8ce3072c962f57a72cd9842984474002c` on `codex/1006-work-account`. Its only changes from that main are AGENTS.md and tests/unit/test_entrypoints.py.

The supplied patch was actually checked and applied in an isolated checkout of that integration head. All four resulting files matched the tested proposal bytes. Reconcile any newer integration changes before applying it.

## Changed interfaces and dependencies

| Path, relative to the repository | Concrete change |
| --- | --- |
| some-implementations/riverhog/recovery/src/a_riverhog_recovery_tool/_provenance_reader.py | New attempt-scoped prepare/view API; reuse existing scan validation; select journal/kind rows from the prepared projection; close bounded source streams; reject buffered yields after owner close. |
| .../_provenance_index.py | Private SQLite descriptor projection with ready seal, keyed journal access and actual resource receiving. |
| .../recovery.py | Make _stage_provenance an owned context through mapping; use exact admitted root bytes; close unreturned journals state and mapping selection/member-cursor state on failure. |
| some-implementations/riverhog/recovery/tests/test_prepared_provenance.py | Small independent fixtures and maintained count, source-integrity, actual-caller, query-access, interruption and close-retry tests. |

No change to package dependencies, lockfile, public exports/CLI, canonical archive/provenance formats, independent decryption, Core reader, neutral history store or semantic journal/history libraries. The private `_stage_provenance` helper changes from returning a tuple to yielding it from an owned context; its single production caller is updated with it.

The recovery-owned projection follows [Core's existing index](https://github.com/nashspence/riverhog/blob/be483fd6d567616d5fcd355202d61f1e682efe75/riverhog/src/riverhog_core/provenance_metadata_index.py). It does not import that implementation or add its server scratch/capacity/concurrency dependencies.

## Claim to evidence

| Claim | Proof used |
| --- | --- |
| Sequence/root authority remains exact | Existing independent scanner reused; new malformed/changed-root/sequence tests; current archive-contract document validators. |
| Binding completeness remains enforced before exposure | Existing binding_tree_commitment pass retained; new independent page-boundary/coverage tests. Do not substitute Core's ordinary iter_bindings implementation. |
| No J×D source traversal or hidden indexed full scan | Old/new actual reader and staging probes; maintained actual-caller source/row counts plus actual keyed query-plan checks. |
| Independent recovery remains independent | Source ownership/dependency-direction guards passed; only standard-library and already declared neutral imports added. |
| Real journal/corpus semantics survive | Probe's actual _stage_provenance uses genuine canonical journals and actual validators; existing encrypted full-recovery tests pass. |
| Selected/inherited history remains complete | Existing encrypted supplementary-history, inherited-history-without-source, and missing-selected-structure tests passed. Neutral MemberHistoryStore/MemberHistoryClosure implementations remain unchanged. |
| Mutable snapshot fences and output behavior survive | Existing metadata-head mutation, trusted expected identifiers, no-clobber and failed-write/retry encrypted tests passed. |
| Partial/interrupted resources are received | New source-close, partial-cursor, buffered-output-after-close, constructor-retention and before/after close-error tests; staging failure closes original state. |

These are finite reuse/experiment claims. They do not promote the old full-source CI result to the new source.

## Apply, verify, continue

1. Fetch/read the exact published reference from the #991 publication record. Review the snapshot semantics and four-file patch. Apply/adapt it on the current authorized integration branch, not by merging the reference branch.
2. Run the focused commands in [VALIDATION.md](VALIDATION.md) with the repository-pinned environment and official age tools. The cheap count/resource test module has no age availability skip.
3. Keep the normal current-source lint/PR CI and installed/platform obligations with the integration owner. No new status name, registry, workflow or numerical authority is introduced here.
4. Do one matched changed-source recovery diagnostic under the real F1/F2 conditions already recorded in #991, keeping publication, independent recovery and receiving boundaries separate. Record actual decryption invocations and complete output. If that comparison does not show the expected useful reduction, attribute the residual before adding mechanisms.
5. Post a finite Proffer disposition against this reference's actual commit: reused/adapted/reproduced/partial/rejected, actual integration SHA and checks, and remaining qualification. Then continue the other #991 accounting work.

The existing broad campaign remains under #1006/#991; #1007 retains downstream whole-path/growth/recovery acceptance. This proposal addresses recovery verification work, not the separately measured publication latency or all finalization costs. It neither freezes the shared all#68 design nor introduces a new global prerequisite.
