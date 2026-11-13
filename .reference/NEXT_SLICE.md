# First integration slice: complete journal caller accounting

Scope: static follow-up at `8ce49de94c32ae39562aab5ea4178b1dc8c75e77`; no product edits or workloads. New measurements are issue-reported, not remeasured here: [#991 checkpoint](https://github.com/nashspence/riverhog/issues/991#issuecomment-6854556220).

## What the new counts establish

The one-member diagnostic reports 400 server validation units, 10,241 Native journal advances, 30,604 checkpoint reads, 40,751 prefix checks and 47,435 SQLAlchemy catalog executions. Those totals cover different scopes. In particular, **47,435 / 400 is not the catalog cost per server unit**, and 10,241 Native advances are not all descendants of those 400 units. Client authorship, fixture history preparation, finalization and all three exhaustive reader passes can create Native sessions.

The selected exclusive CPU figures locate a useful starting scope, but neither their ranking nor the counts establish avoidability. The reported 202 readiness-transition calls with one actual readiness check specifically warn against equating frequent callers with repeated expensive child execution.

## Exact server validation owner and its callers

The selected owner is:

[riverhog/src/riverhog_core/services/collection_uploads.py](https://github.com/nashspence/riverhog/blob/8ce49de94c32ae39562aab5ea4178b1dc8c75e77/riverhog/src/riverhog_core/services/collection_uploads.py) `SqlAlchemyCollectionUploadService._advance_provenance_journal_validation_unit`.

Its two immediate invocation routes go through `_advance_provenance_journal_validation`:

- `seal_provenance_journal`: capture starts, then attempts one stamped service advance.
- `process_due_provenance_journal_validations`: bounded scheduler selection, stale-owner release/retirement and active/fresh-key selection, then a stamped advance; on durable progress it calls `_schedule_finalization_if_ready`.

The stamped wrapper samples `_upload_journal_validation_stamp` before and after the actual unit. It owns the comparison; child work is independently declared.

A unit follows this straight-line, early-return chain:

1. Read pending upload capture; if present, perform detached bounded framing and commit its result.
2. Read current validation work.
3. `upload_journal_epochs.advance_upload_journal_epoch_scan`.
4. `upload_journal_topology.advance_upload_journal_topology_capture`.
5. `upload_journal_union.advance_upload_journal_epoch_union`.
6. `upload_journal_rank.advance_upload_journal_epoch_rank`.
7. `upload_journal_replay.prepare_upload_journal_replay`.
8. `UploadJournalValidationOwner.advance`.
9. If terminal, commit the exact outcome to catalog state.

Completed prerequisite calls still run on later units, but can return through ready/probe paths. Count their actual SQL and child work rather than classifying the call itself as repeated preparation. For example replay preparation constructs the exact binding, checks rank state, then `_commit_binding` performs fenced catalog reads even when the retained binding already matches.

Within `upload_journal_validation_owner.py::UploadJournalValidationOwner.advance`, a warm owner calls `current.progress()`, then `current.advance(budget)`, then `jobs.publish_progress`. The progress and advance paths both retain current-source checks. `_OwnedSource.verify_current` composes `jobs.worker_current`, `UploadJournalValidationSource.verify_current` and `UploadJournalGraphCertificates.verify_current`; source read methods require the same current fence before reading. These checks protect exact worker/source/certificate authority. Their multiplicity and cost are attribution targets, not permission to remove them.

## Existing maintained evidence — the precise composition boundary

The service declarations are already source-owned:

- `_service_validation_unit_declaration`: `riverhog-server.validation-service-unit.growth/v1`, with exclusive `core_*` primitives and mandatory Native child samples.
- `_service_validation_advance_declaration`: `riverhog-server.validation-service-advance.growth/v1`, at most two stamp children and one unit child.
- `_service_scheduler_declaration`: scheduler children include sweep, active keys, retirement, catalog selection, stamped validation and readiness scheduling.
- `upload_journal_validation_owner.py::_advance_declaration`: `riverhog-server.validation-owner-advance.growth/v1`, including Native advance, current fences, progress and publication.

Relevant maintained tests:

- [tests/unit/test_upload_journal_service_unit_growth.py](https://github.com/nashspence/riverhog/blob/8ce49de94c32ae39562aab5ea4178b1dc8c75e77/tests/unit/test_upload_journal_service_unit_growth.py) `test_real_validation_service_units`.
- [tests/unit/test_upload_journal_service_caller_growth.py](https://github.com/nashspence/riverhog/blob/8ce49de94c32ae39562aab5ea4178b1dc8c75e77/tests/unit/test_upload_journal_service_caller_growth.py) `test_actual_stamped_service_reports_durable_progress`.
- [tests/unit/test_upload_journal_service_coordination_growth.py](https://github.com/nashspence/riverhog/blob/8ce49de94c32ae39562aab5ea4178b1dc8c75e77/tests/unit/test_upload_journal_service_coordination_growth.py).
- [tests/unit/test_upload_journal_validation_advance_growth.py](https://github.com/nashspence/riverhog/blob/8ce49de94c32ae39562aab5ea4178b1dc8c75e77/tests/unit/test_upload_journal_validation_advance_growth.py) and existing source/checkpoint/Native growth tests.

**Crucial coverage detail:** the first two tests deliberately advance their Native fixture directly, outside the measured service call, to selected pending-frame and terminal boundaries. Both assert exactly **three measured Native-owner calls**. This is legitimate unit/child evidence and remains valuable, but it does not census the normal caller cost for every Native transition of a complete journal. The unit declaration explicitly excludes total scheduler/publication qualification.

## Recommend one finite next slice: a complete small journal through the real stamped service caller

Use the maintained small journal fixture and its existing `Recorder` / `UnitRecorder` machinery. Receive one exact journal from capture to terminal publication **without advancing its Native session directly between service calls**. This is a targeted owner witness, not another whole-publication matrix.

Record each actual service attempt, exact attempt/source/worker identity, prior/next durable phase and Native transition, whether progress happened, and actual child invocations. Charge catalog SQL to:

- before/after stamp snapshots;
- capture/validation metadata reads;
- epoch/topology/union/rank/replay ready probes versus genuine preparation;
- stale-owner/reservation and job/source/certificate fences;
- Native progress publication and terminal commit.

Record direct Native SQLite and admitted source bytes in separate child scopes. Preserve exclusive parent counters and sum children only in an explicitly composed subtotal; do not double-count wrappers. Keep unclassified catalog events visible instead of allocating the full-profile 47,435 calls by arithmetic.

**Finite output:** one complete original-owner receipt; a stage-by-stage statement/returned-row/byte and wall/CPU ledger, with engine-work counts only where actually supported; total actual unit and Native-transition counts; attribution coverage and residual; and a specific conclusion about whether the warm service path is spending significant work on unchanged prerequisite probes, necessary current fences, Native execution, or another identified cost. Retain the actual cleanup of Native readers/workspaces and the original service owner. Reuse existing interruption/stale-source tests when a proposed reduction changes their boundary.

This closes a concrete measurement/composition gap exposed by the new counts. If it demonstrates redundant work, the integration owner can choose one semantics-preserving correction and a meaningful maintained regression. If the work is required, record that rationale and its actual cost, then select the next owner from the ledger. It does not require a new global design study, another broad one/two/four profile series, a changed concurrency limit, or a larger trial.

One complete small journal is **still not a full growth proof**. Later owner-directed independent input dimensions must establish the appropriate total-work envelope; current source-owned formulas and correctness/security/custody requirements remain controlling.


The independent count witness and its observer limitations are in [RESULTS.md](RESULTS.md). It supports the priority of this slice without replacing the current integration checkpoint or establishing a new acceptance requirement.
