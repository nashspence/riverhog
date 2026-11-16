# Qualification and finite integration sequence

This document distinguishes the reference's executed checks from the **product evidence still required by #995**. The source baseline is `codex/964-session-owner@0f8030b0c0cd53eb33456924f070ae5979ed307e`; product paths in this reference match it. Current issue decisions and the exact integration source must be reconciled before adoption. #994/#995, not this file, own acceptance.

## Reference reproduction

From the repository root, using Python **3.12.3** as pinned in `mise.toml`:

```bash
python3 -m unittest discover -s .reference/implementation -p 'test_*.py' -v
ruff check .reference/implementation
ruff format --check .reference/implementation
```

Use Ruff **0.16.0**, the audited `uv.lock` version. [VALIDATION.md](VALIDATION.md) records the actual final run, versions, hashes and blockers. The Python tests use only the standard library. They are deterministic policy and owner-state simulations; no filesystem generation, real database CAS, resource receiving, authenticated transport or power-loss guarantee is established by them.

The model deliberately receives owner-proven facts as inputs. A boolean proof in the simulation is not an authentication or durability implementation. Its root is single-writer; production must implement the specified transactional CAS against the current root. Raw model summaries are not untrusted wire data and are not a production deserializer. The neutral policy module is concrete proposed reusable code; the accounting model is an executable protocol specification to translate into existing owners.

## First focused integration step

**At the current #979 source, wire the shared policy and surviving root into `UploadJournalValidationOwner`, and replace unconditional `jobs.publish_progress()` after each Native advance with coalesced live processed progress and selection/result-only adoption.** Pair this change with #996's private executor/generation contract and #997's F1 publication path; a guard around one SQLite commit is insufficient.

Use this finite order, preserving useful WIP:

1. Reconcile #994/#995 and current #979. Reuse the reference module in the neutral package, add actual consumer dependencies, typed packaging, source-owned declarations and strict configuration tests. Keep the reference documents/model out of the product tree.
2. Add snapshotted policy and bounded accounting to `ArchiveWriteControlRecord` at existing construction admission; implement grant CAS, selection/count, cumulative-cost/tail settlement and uncertainty recovery. Preserve `construction_json`, crypto/effect identity and authority. Keep useful elapsed volatile between existing anchors/publications.
3. Integrate the coordinated Native/Core seam. Cheap work uses the same complete validator with zero optional store/checkpoint/adoption. Preserve bounded private scratch, current source/worker/auth checks, cancellation and original receiving.
4. Implement finite owner/backend estimate profiles through maintained `make profile`. Exercise an actual admitted long/unknown-length candidate, dependency persistence, root selection, kill/restart and fallback. Do not make a permanently unknown policy the final product.
5. Complete parser/schema/runtime/API/SDK/CLI/state/generated/installed changes together. Verify defaults, overrides, new-attempt-only changes and independent client/server authority through behavior, not just field discovery.
6. Run the scoped failure/growth/installed/backend witnesses below and exact-source required qualification. Publish a finite consumption/evidence disposition on #995; leave parent-wide completion/#991 resumption with #994/#1000.

This sequence is implementation guidance, not six serial PRs, a complete repository census prerequisite or a new acceptance authority.

## Deterministic model and configuration witnesses

The reference covers exact 120-second boundaries, required completion/anchor priority, unknown versus zero estimates, restore subtraction, expensive snapshots, local and aggregate inequalities, strict type/range validation, zero fraction/count, retention/count separation, held grants and unknown/debt suppression. Composition cases cover elapsed overlap, retries, count slots, stale decisions, original publication reconciliation, failed attempts, actual/tail cost replacement, terminal receiving and older-data/newer-budget recovery.

Move these invariants into maintained package/owner tests when integrating. Add installed behavioral tests using actual owner decisions for:

- Defaults and each override; numeric booleans/strings/nonfinite values rejected before coercion.
- Same idempotent construction after a server configuration change retains old effective policy; a new construction uses the new policy.
- Local SDK/CLI restart suppresses local optional work; it does not change server policy or mandatory prepared state. Caller-supplied elapsed/grants/policy cannot mint server authority.
- `max_optional_checkpoints` counts selected successful optional generations across retries, phases and pruning; held ambiguous slots block new admission.
- Status inspection distinguishes unavailable processed progress, selected resumable frontier and uncertain accounting without writes/payload reads.

Existing policy owners include `tests/unit/test_runtime_config.py`, `test_configuration_connectivity.py`, `test_workspace_boundaries.py`, `test_dependency_policy.py`, `test_public_interface_parity.py`, `test_response_contract_exactness.py`, `test_cli_parser_contracts.py`, `test_cli_result_contracts.py` and `test_contract_freeze.py`. Regenerate `config.schema.json` from the actual parser and generated public contracts through `make contract`/`make contract-check`; changing generated expectations never authorizes a protected change.

## Real owner fault matrix

| Area | Required witness and interpretation |
| --- | --- |
| Cheap successful work | Actual authenticated, encrypted F1 publication and fresh/warm/evicted independent Native readers in restart/default adaptive. Zero optional generations and no optional resume-store initialization; complete same captured semantic output/custody. |
| Long or unknown input | Same executor earns real useful work, forms finite profile-labelled estimates, promotes, publishes a usable generation, is interrupted and resumes or safely falls back. An artificial clock test does not replace actual storage/restart. |
| Root admission | Competing revisions, capacity refusal, failed pre-grant CAS, commit loss and changed estimates. No candidate starts before authoritative grant; lost unrecorded cost seals optional admission. |
| Candidate generation | Crash before/after dependency persistence, before/after selection, partial child state, scratch mutation after selection, disk full and corrupt/truncated state. No mixed cursor/semantic state; previous usable generation survives. |
| Publication/reconciliation | Lost selection acknowledgement, exact generation identity, stale worker/source/auth, count increments once; known cost alone cannot prove nonpublication. |
| Cost/retention | Failed attempts and cleanup charged; overrun is actual cost, not a second debit. Cleanup-pending publication stays counted. Pinned predecessors/extras remain until actual receiving; storage refusal cannot delete necessary dependencies. |
| Crash budget | Latest surviving root summary with older fallback data; unknown executed high-water earns no duplicate credit; no new root on retry/phase/worker change. Test absent grant with possible failed admission work. |
| Mandatory prepared evidence | Stage/bind/decision acknowledgement loss preserves original journal bytes, observations, UUIDs/timestamps, input/history/completion binding and attribution. No incompatible re-authoring. |
| Effects and custody | Exact original encryption/request identity before dispatch; ambiguous begin/write/seal, late writers, cancellation, lost catalog commits/replies, changed permissions and source release only after exact required custody. |
| Terminal tail | Final user-visible publication deletes upload row but surviving control retains grants/actuals/cleanup. Closed gate cannot admit new optional work; exact original receiving continues. |
| Independent progress | One workflow's computation/provider waits do not hold a shared catalog writer or stall unrelated work. Status/yield/cancellation do not require optional checkpointing. |

Retain the decisive safety cases in these existing owners (relocate injection boundaries when necessary):

- Native: `packages/riverhog-provenance/tests/test_journal_session.py`, `test_retained_journal_session.py`, `test_validation_checkpoint.py`, `test_validation_checkpoint_rejection_growth.py`, `test_entry_replay.py`.
- SDK: `tests/unit/test_prepared_primary_publication.py` and `packages/riverhog-client/tests/test_prepared_primary_store.py`, `test_prepared_primary_barrier.py`, the SQLite/native/VFS/POSIX/Windows prepared-primary suites.
- Core: `tests/unit/test_upload_journal_validation_owner.py`, `test_upload_journal_validation_custody.py`, `test_archive_write_admission.py`, `test_archive_write_execution.py`, `test_archive_catalog_publication_receiving.py`, `test_archive_finalization_pipeline.py`, `test_upload_terminal_receiving.py`.
- Backends/concurrency: the existing collection-finalization, upload-custody, artifact-custody-receipt, immutable-source-receiving and cleanup-receiving integration suites.

`test_preparation_and_terminal_records_do_not_cascade_away_with_upload` is the existing witness for the chosen surviving root. Extend it to policy/accounting and unfinished cleanup. `test_original_crypto_is_committed_before_provider_begin` and the actual HTTP stage/bind/decision-loss prepared-primary witness remain decisive mandatory-barrier checks.

Deliberately rebaseline tests such as `test_retained_session_resumes_its_exact_bounded_unit`, `test_actual_process_exit_retains_source_and_parent_cursor` and `test_real_worker_exit_adopts_committed_prefix_before_catalog_publication`: private every-step durability is superseded. Move interruption to the new selected-generation/mandatory boundaries and prove the same semantics after replay. Distinguish lost optional state from lost accepted authority. Preserve error/semantic/custody assertions; do not simply delete failures or freeze the old phase count under a new name.

## Growth and maintained qualification

Use existing operation/delegate discovery and state/configuration/installed-component discovery. Update Native checkpoint/entry/session/store/corpus and Core validation-owner/job declarations together with their caller composition. The neutral policy has fixed-shape decisions over bounded values; owner composition is bounded by admitted active grants. Account integer/control widths honestly rather than assuming free arbitrary-precision work.

Measure complete lifecycle work: execution, candidate setup/capture, hashing/encoding, dependency persistence, selection, restore verification, compaction, failed attempts and reclamation. Independently vary member, byte, history, row, relation and width dimensions. A per-step bound does not excuse repeated full-prefix copies/rehashes, unbounded restore chains or quadratic lifetime work. Observe short shared writer ownership, original resource receiving and real cleanup.

Use the repository-pinned toolchain and Make evidence path. Typical focused development commands are:

```bash
make lint
make unit TESTS='packages/replay-policy/tests packages/riverhog-provenance/tests tests/unit/test_upload_journal_validation_owner.py tests/unit/test_upload_journal_validation_custody.py tests/unit/test_prepared_primary_publication.py tests/unit/test_archive_write_admission.py tests/unit/test_archive_catalog_publication_receiving.py tests/unit/test_runtime_config.py tests/unit/test_configuration_connectivity.py'
make contract-check
```

The package path above is the proposed integrated package. Select additional changed-owner tests through the existing source-owned qualification declarations, not by treating this list as a new exhaustive registry.

At final integrated handoff, the audited `AGENTS.md` requires complete passing required checks for the **exact final source and tested merge checkout**. PR checkpoint pushes use lint and focused owner tests; full local qualification, when needed, is `make linux-qualification`. Applicable lanes include actual Linux/macOS/Windows Native/installed tests, SQLite/PostgreSQL, images/Compose, distribution smoke and independent filesystem recovery. Record every skip, block and original job/artifact identity. Reference tests, prior #979 evidence and another source's green CI cannot substitute. #993's previously reported macOS capacity gaps retain their actual source/scope; inspect current evidence rather than inheriting a pass.

## Matched measurements through the existing entry point

Use `make profile` and the maintained collection-publication geometry. One existing command form is:

```bash
make profile args='--scenario collection-publication --publication-workload /absolute/workload.json --output /absolute/profile-output'
```

The workload document requires `format: riverhog-publication-workload/v1`, exact `payload_sizes`, `shared_root_members`, `shared_claim_members`, `retained_journals` and `required_journals`. Keep matched F1/F16 and maintained processing/lifecycle conditions; do not change the payload/resource deadline to manufacture a comparison. Add the new policy/estimate observations and supported long generation witness to this existing profiling path, with their implementation under the owner. No separate profiling CLI or new durable telemetry database.

Compare preserved old implementation A with new executor restart B and the **same new executor** adaptive C. Keep semantics, source geometry, mandatory safety, backend, resources, cache conditions and completion definitions equal. Do not use A as the denominator for optional overhead. Report repeated paired `D=max(0,T_C-T_B)`, `D/T_C`, conditions and uncertainty; at the selected default the observational target corresponds to `D <= T_B/7`.

Separate publication, each independent verification pass, time to durable result and time to actual terminal receiving/reclamation. Report summed CPU/work/storage separately from elapsed time. SQL/commit attempts are not physical fsync counts. Count optional setup, instrumentation, failed CAS/candidate attempts and tail cleanup; do not hide parallel/background costs. Interrupted cases separately report lost useful work, restore/revalidation, repeated bytes/effects and time to eventual valid completion.

The existing publication profile uses TestClient HTTP, SQLite, an in-memory ciphertext store and explicit cache observations; it does not qualify installed offline recovery or production RAM. Run those maintained owners separately. Preserve the source/geometry/deadline/clean-tree evidence checks. Do not start held 1,000/10,000/100,000 campaigns or real-provider qualification to calibrate this first slice.

The stopping rule is scoped and factual: #995 can be offered for acceptance only after the common policy/configuration/accounting is exercised by a real owner path, cheap replay avoids optional machinery, supported long work promotes and recovers, mandatory safety and bounded growth remain, and applicable exact-source evidence is received. Arithmetic passing, fewer checkpoint counts or a favorable single timing is insufficient. Parent-wide coverage and permission to resume #991 remain with #994/#1000.
