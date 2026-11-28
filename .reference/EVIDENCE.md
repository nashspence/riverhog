# Completed-preparation finite work account

Source baseline: `60612eded1e0e4e5188c28a84697435eb969fad0`.
Candidate production: `collection_uploads.py` SHA256 `70af3f63e1ea4b5d8b7cb064862911d295143cdeee39e463f0c95fee883fec2f`; `upload_journal_replay.py` SHA256 `293b5b302f3e50cf8de105fda32081675a70c43c6bcae78485c374dc0cf291a0`.

## Executed path

The finite probe calls the actual `SqlAlchemyCollectionUploadService._advance_provenance_journal_validation_unit` until its actual terminal journal payload is sealed. It uses real file-backed SQLite catalogs, captured source segments, epoch/topology/union/rank preparation, official Native session construction and every real Native turn. No Native turns are bypassed or advanced by a substitute executor. Fixtures start before capture completion. The default `JournalValidationBudget` is unchanged in both arms.

Six fixtures contain exact shared journal bytes, saved once and passed to both processes: 1/2/4 valid frames, one additional correction frame producing a second epoch, a one-frame journal with 65,537 extra text octets, and the same one-frame input with 1,025 unrelated retained journal rows. The correction case changes both frame count and epoch count, so it is not an isolated estimate of epoch cost.

`experiments/preparation_probe.py` emits complete per-turn observations and before/after hashes for eight production owner files, exact fixture hash, selected imported-module paths, the unchanged default Native budget, and native-extension/build-source hashes. The probe uses the pinned dependency environment described in [VALIDATION.md](VALIDATION.md) and an existing same-source `retained_resource._native` build output, copied into each local source checkout. The native binary SHA256 is `10c7998c103c7c3a714ac2e39fcc58632576e2c624279fbc9573cfdaa6c7832e`; its source inputs are byte-identical in baseline/candidate. The probe asserts imported representative owner modules are under the selected checkout.

## Results on final production bytes

Use the compressed raw observations in `evidence/preparation-baseline.json.gz` and `evidence/preparation-candidate.json.gz`, and the compact verified [comparison](evidence/preparation-comparison.json). `experiments/compare_preparation.py` independently checks every pair and reproduces the summary.

| Fixture | Native turns, both arms | Catalog execution events before → after | Catalog SELECTs before → after | Commit events before → after |
| --- | ---: | ---: | ---: | ---: |
| 1 frame | 107 | 5,624 → 4,672 | 3,954 → 3,001 | 771 → 453 |
| 2 frames | 141 | 7,422 → 6,166 | 5,219 → 3,961 | 1,016 → 596 |
| 4 frames | 209 | 11,035 → 9,173 | 7,762 → 5,895 | 1,508 → 884 |
| 5 frames, including one correction | 381 | 21,549 → 17,385 | 15,633 → 11,461 | 2,739 → 1,599 |
| 65,537 extra text octets | 124 | 6,522 → 5,417 | 4,584 → 3,478 | 891 → 522 |
| 1,025 unrelated journals | 107 | 5,624 → 4,672 | 3,954 → 3,001 | 771 → 453 |

Catalog execution events mean SQLAlchemy's actual `before_cursor_execute` events, including explicit snapshot `BEGIN`; they exclude separate commit/rollback events. They are not complete driver-level primitives or Native SQLite statement counts. They cover the validation-unit calls, while final service-close receiving is asserted separately and its primitives are outside these counters. The observation records retain both total catalog work and the service/Native call-stack distinction. Timings are intrusive advisory observations and were not controlled for overlap with other validation. They establish no production elapsed saving.

All six candidate fixtures invoke no preparation advancement helper after first Native admission. For the one-frame input each of epoch scan, topology capture, union advance, rank advance, replay preparation and binding publication goes from 107 invocations to one. Longer inputs still execute their necessary bounded preparation passes before that first admission; the four-frame case invokes epoch/topology five times, union twice and rank/replay once.

This is consolidation, not removal of all preparation checks. One-frame total epoch and topology restorations remain 107 each. Total union restorations drop 328 → 116 and rank restorations 220 → 114. Counts include real first preparation and independently owned cold Native graph-certificate construction.

A representative warm one-frame service turn retains one epoch and topology restoration, reduces union restoration from three to one and rank restoration from two to one, and omits the completed binding publication transaction. Two extra exact binding point probes are paid. Service-owned SELECTs drop 27 → 18 and commits 6 → 3. Its actual Native source/worker/certificate/checkpoint/progress work remains unchanged.

All pairs have exactly the same Native turn count and selected wrapper counts for Native advancement, session advancement, source currentness, worker currentness, graph-certificate currentness, checkpoint load/fence, progress and publication. Every published terminal anchor matches an independently validated expected journal. All original Native construction records report receiving, every captured original session reports storage received, and no catalog connection is checked out after any caller turn or final service close. The owner has no remaining slots after close. Durable Native snapshots are separately listed before fixture cleanup; the enclosing test directory is then removed and its absence asserted.

## Source and execution history

An initial exploratory probe omitted the explicit `_commit_binding` counter and native-build identity fields. Its observations are retained under `preparation-baseline-preliminary.json` in `evidence/development-observations.json.gz`; they are not the final comparison evidence. The earlier candidate is retained there under `preparation-candidate-final.json`.

The final probe was run against baseline, then against the proposal before one Ruff-only caller line wrap, then against the exact final production files. The final comparison uses only `evidence/preparation-baseline.json.gz` and `evidence/preparation-candidate.json.gz`; its source-file hash guards passed. Earlier candidate counts are identical. No acceptance limit, work budget or caller condition changed in any comparison.

## Growth witness change

The existing `test_upload_journal_service_unit_growth.py` recorder now counts the five real preparation-advancement helpers and asserts they do not repeat after first Native admission on its existing real service cases. It retains the maintained Native descendant observations, currentness, resource receiving, population growth, interruption/rollback and bounded preparation-window coverage. The source-owned service-unit declaration adds an exact explanation of positive-binding admission and retained checkpoint/last-record checks. Its numerical bounds, resource conditions and delegates do not change.

The full maintained samples are in `evidence/growth-samples.json.gz`; exact case identities and the formatting-only source relationship are in [evidence/validation.json](evidence/validation.json). This witness has its own maintained fixture acceleration to isolate three real service-call boundaries; it is separate from the unaccelerated six-fixture probe above. The probe, rather than that fixture acceleration, establishes the complete actual Native turn counts.

All five tests in the maintained growth file passed in 329.05 seconds. They recorded 20 top-level samples (10 actual service units, four absence/rollback/retry units and six bounded preparation windows), plus the two import-order test cases. The exact records are in `preparation-growth-results.xml` and extracted `preparation-growth-samples.json`. During that run, Ruff wrapped one caller expression; replay and test files are unchanged. `preparation-growth-source-relationship.json` reconstructs the recorded earlier caller byte-for-byte and verifies full AST equality with the final caller. This is a formatting-only relationship, not evidence that unexecuted logic passed. The initial xunit2 JUnit invocation had three setup errors because `record_property` requires xunit1 under the repository's warnings policy; its two import-order cases passed. That invocation is retained separately, and the five-case successful run used xunit1.

## Limits

The experiment is a SQLite actual-caller work count, not a PostgreSQL runtime execution, installed build qualification, production-KDF publication diagnostic, full archive operation, maximum-duration acceptance or per-file marginal-cost measurement. The correction fixture does not isolate epoch cost. No full algorithmic/whole-operation growth bound follows from these small observations. Remaining source, checkpoint, last-record, Native graph-certificate and progress-publication costs stay explicit; the agent should not subtract the prior inclusive helper attribution as guaranteed elapsed savings.

## Counterexample evidence

[The cold-admission probe](evidence/native-audit-probe.json) reproduces two exact-baseline metadata counterexamples and records compilation of the existing point query for both SQLite and PostgreSQL. It does not execute PostgreSQL or publish a semantic result from damaged preparation. Run `experiments/native_audit_probe.py` against an unmodified audited baseline to reproduce it. [DESIGN.md](DESIGN.md) explains why the retained checks address the gap.
