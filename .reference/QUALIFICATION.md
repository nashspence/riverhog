# Qualification and cutover for collection publication

**Non-authoritative #903 handoff for [#997](https://github.com/nashspence/riverhog/issues/997).** This document specifies integration evidence; it does not claim the proposed production cutover is implemented. The observed F1 result below is an unchanged-source baseline. Reference tests, source hashes and actual execution limits belong in [VALIDATION.md](VALIDATION.md).

Audited source: [#979](https://github.com/nashspence/riverhog/pull/979), `codex/964-session-owner@c28eceaa2dfec1b3712135469ba12230e74f5ffa`. Reconcile later source and accepted decisions before integrating. [#994](https://github.com/nashspence/riverhog/issues/994), [#997](https://github.com/nashspence/riverhog/issues/997) and [AGENTS.md](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/AGENTS.md) control. The [#995 input at b545a779](https://github.com/nashspence/riverhog/blob/b545a77996fbc9fe98019796860473bda1bb1aa0/.reference/HANDOFF.md) and [#996 input at e4a60708](https://github.com/nashspence/riverhog/blob/e4a607081d17ae1a067bb766241bd5febd9ddeb9/.reference/HANDOFF.md) are concrete proposals to reconcile, not installed dependencies or new acceptance authority.

## 1. First production witness

Implement the common #996 journal executor and generation/result-only Core adoption together with #995 policy/accounting on the real SDK → authenticated API → encrypted archive F1 path. Preserve complete member/history/index/custody verification, then exercise fresh/warm/evicted independent readers. Prove that cheap replay creates no optional recovery generation or storage machinery. Mandatory original evidence, effect intent, exact receipt and final publication remain durable in both `adaptive` and `restart` modes.

After that cheap slice, demonstrate a real long/unknown-remaining-work candidate and process interruption on the same executor/owner path. Continue the remaining publication, discovery/corpus and application consumers through their owning contracts. A synthetic clock or a page-store model is useful for deterministic tests but cannot establish actual default-policy eligibility, Native recovery or end-to-end publication.

This ordering follows [the parent breakdown](https://github.com/nashspence/riverhog/issues/994#issuecomment-6921730895). It does not require completing the whole-parent census first. It does not resume the held #991 campaigns, impose a fixed commit/lifecycle-state count or make every old microstep mandatory.

## 2. Acceptance matrix at actual owners

| Owner/boundary | Required witness |
| --- | --- |
| SDK construction and creation | Lost create reply reuses the original request/construction identity and exact accepted binding. Conflicting parameters under that identity are rejected. A retry does not mint a new construction to erase uncertainty. |
| Prepared-primary winner | Capture original observations, UUIDs/timestamps, selected history/configuration, authored journal bytes and completion obligations before staging can become ambiguous. Stage success followed by bind/decision-ack loss reuses those exact bytes. Source payload bytes alone are not sufficient recovery input. |
| SDK control observation | Establish local durability knowledge only through successful mandatory execution/recovery, invalidate it before any mutation, and reuse it only for the same current owned generation. Warm and cold status polls perform no sync/recovery; unknown state exposes advisory metadata and delegates recovery to execution. Release the mutex before network work, waits and subsequent polls. Mutation, exception, ownership transfer and ambiguous completion invalidate the proof. Preserve both adapters' per-member mandatory barriers. |
| Bounded mandatory grouping | Every item in a proposed group is independently recoverable before any member dispatch or acknowledgement that depends on it. Crash before/after the shared commit gives the permitted old/new outcome; a provisional group is not reported committed. Group admission includes actual byte/resource extent, not only item count. |
| Native execution and Core adoption | Multiple semantic units execute privately within one aggregate bounded quantum. Ordinary yields and progress polls cause no recovery codec round trip or second Core projection. Current source/worker/authority checks remain at admission/effect/result boundaries. Only selected complete generations and mandatory semantic outcomes are durably adopted. |
| Registration/planning/history/index construction | Reproducible construction remains private/bounded until a complete consistency boundary. Final publication requires all current payload, provenance, history, semantic, index and root dependencies. A staged object or saved cursor does not certify unvalidated semantic work. |
| Registration plan handoff | Request/member acceptance and registration acknowledgement remain mandatory. Stage/verify/audit cursors stay private. Import a complete plan into its hidden generation in bounded batches, then atomically acknowledge registration and select the matching planner generation. Do not classify earlier resumable cursors as required final row installation. |
| Index seal and publication | Prepare complete index proof through bounded read-only work outside the shared writer. Every generation-content mutation advances `content_revision` in that same batch transaction. A typed seal candidate and its stored completion result bind exact generation, content revision, worker epoch, publication input revision and archive root. Seal acceptance CAS-validates those bindings. Initial publication separately rechecks current authority and the same source binding, then selects the complete index pointer and sets `CollectionRecord.is_published` together. Counts alone are not completeness. A content mutation between prepare and accept must invalidate the candidate even when row counts stay unchanged. |
| Optional policy and accounting | One surviving construction budget across SDK/Core/Native, retries and workers. Finite grant before optional setup/encoding/capture; one short fenced selection/count/frontier transaction after complete Native persistence. Held grants/count slots, actual charges and successful selections remain distinct. |
| Archive effect preparation/arming | Original key/nonce/ciphertext continuation, exact target/request and current invocation identity are durable before provider mutation. A cold observer cannot manufacture live dispatch rights. Replayed/duplicate calls reconcile that original outcome; different content cannot reuse crypto continuation. |
| Returned effect report | Exact returned result and corresponding catalog change commit together at the existing owner. Commit loss, reply loss and provider failure have separate witnesses. A possibly successful original cannot be replaced or reclaimed based on timeout or a missing reply. |
| Early receipt and final publication | Early custody retains its existing narrow scope. Complete exact receipts, closure and semantic/index verification precede atomic authoritative root/catalog publication. Receipt readback and source release follow their exact authenticated owner contract; neither a mutable upload row nor optional frontier becomes archive authority. |
| Index failure after sealing | A failed/interrupted index build leaves sealed archive objects and exact custody receipts available under their original owner. It cannot declare publication complete, discard sealed custody or regenerate already accepted evidence. Rebuild only private derived state after fresh admission. |
| Terminal receiving and reclamation | Original worker/file/SQLite/provider handles remain reachable and reserved through actual return, close/join and receiving. Terminal selection may fence new effects but cannot prove an old worker stopped. Failed cleanup keeps the original and its charge; do not hide the tail after declaring full settlement. |
| Public activation and due work | Exercise actual authenticated upload/cancel/discard routes with pre-body reservation, original input transfer, exact dispatch and terminal selection active together. Wire terminal execution into maintenance, including shutdown/receiving. Published constructions reject new forward admission and forward due work while original result receiving, terminal settlement and bounded retirement remain schedulable. A status poll must not be needed to make progress. |
| Publication receipt and retirement | Final publication atomically retains the original publication tuple, initial complete index seal and input revision in `publication_receipt_json`, plus `upload_retirement_pending`. Lose that reply, finish bounded retirement, legitimately rebuild the active index, then receive the same original publication receipt. Later index selection cannot rewrite that historical receipt. Upload deletion waits for required child retirement and cannot strand receiving or erase mandatory acknowledgement/accounting. |
| Cold derived results | Cheap completion freezes the live DB for independent result readers and persists required exact-source outcome, without a forced final optional snapshot. Only derived cache absence/corruption yields `DerivedNativeCacheUnavailable`; the existing owner coalesces bounded revalidation and shares the admitted rebuilt view. Status/member queries cannot repeatedly replay a journal. |
| Source/authority/error classification | Wrong source/history/anchor/tail/configuration/algorithm/worker/permission is rejected at the applicable boundary. A missing/corrupt mandatory source, outcome, intent or custody record retains its error classification and never becomes successful absence or a generic cache miss. |
| Responsiveness and independence | One collection's computation/provider wait or unconfirmed receiving does not block independent admitted collections. Shared writers are short; no provider wait, full journal replay or archive assembly occurs inside them. Status/cancellation coordination does fixed bounded work without waiting for execution. |
| Independent readers and recovery | The same captured evidence produces complete equal history/materialization in fresh/warm/evicted readers. Installed independent recovery uses archive and separately held keys without Core/catalog/checkpoint dependencies. Verified immutable bytes may be reused; positive authorization may not be cached. |

## 3. Mandatory failure windows and preserved tests

For the retained-upload publication design, inject final commit-reply loss while the upload parent still exists: `ArchiveCatalogFinalizer` must receive its already captured publication receipt without relying on `inspect(upload).deleted`, and registry feedback must not requeue forward finalization. Receive an original member-custody reply while the parent remains and its dependency rows are pending retirement; exact published readback or original pins must preserve that reply. No retirement marker authorizes deleting a row still needed by an original receiver.

Keep discard planning read-only: assert it does not close admission or install a terminal selector. Test revoked public read permission after terminal confirmation: public result readback must fail as required, while private `receive_completion` still receives the exact original target without exposing result data or authorizing disposal. Original report rollback/retry must remain receivable after terminal selection; it must not acquire a new forward epoch or require the closed gate to reopen.

For each changed boundary, test before effect, after effect but before durable acknowledgement, and after durable commit but before reply delivery. Retain exact originals across failure. Include actual process death, stale/concurrent workers, changed authorization/source, capacity exhaustion and failed original close/join; exception injection alone does not certify OS-crash/power-loss durability.

| Existing production tests | Preserve or deliberately rebaseline |
| --- | --- |
| `tests/unit/test_prepared_primary_publication.py::test_durable_primary_replays_after_actual_http_stage_bind_and_decision_acknowledgement_loss` | Preserve the actual authenticated staging/binding/ack-loss retry and byte-identity counterexample. Batching may change internal commits, not the original prepared winner. |
| `test_shared_producer_restarts_exact_preparation_and_drains_finalized_custody`, `test_authenticated_custody_survives_lost_decision_ack_and_damaged_local_journals` in that file | Preserve original preparation on restart, authenticated custody, damaged local-cache disposition and actual final receiving. Do not substitute a mock receipt or a policy-only test. |
| `packages/riverhog-client/tests/test_prepared_primary_{documents,store,runtime,factory,native,posix,windows}.py` and SQLite/VFS resource tests | Retain exact immutable preimages, original resource reservations, cold recovery, storage identity and actual platform-specific receiving. Update any changed schema/fields coherently. |
| `tests/unit/test_archive_write_admission.py` | Preserve preparation-before-arm/cold retry, no dispatch without preparation, one-shot capability, original result/catalog atomicity, concurrent terminal selection, actual dispatch-process exit, current authority and crypto-before-provider-begin. Preserve state surviving upload/volume retirement. |
| `tests/unit/test_archive_payload_unit.py` | Preserve early candidate retention, original checkpoint/ciphertext/receipt identity, selected original cache receiver and independent work while another constructor is held. Reclassify derived progress only; encrypted external-effect continuation remains mandatory. |
| `tests/unit/test_upload_journal_validation_owner.py` | Preserve source/rank/worker/lease refusal, capacity pause, independent journals, original result receiving and actual worker exit. Rebaseline exact-every-microstep adoption/version tests to the last selected consistent generation or mandatory semantic result; replay after that frontier is allowed. |
| `tests/unit/test_archive_catalog_finalizer.py` | Preserve exact original batch/cursor after transaction failure, commit/reply loss and workflow feedback loss, terminal receiving without new work, actual Native receiving and responsive polls during an original transaction. |
| `tests/unit/test_archive_finalization_pipeline.py` | Preserve original closed report before later stages, commit-reply-loss reconciliation, charged unconfirmed constructor and actual report/constructor receiving. Phase traces may change with the new private executor; required dependency order may not. |
| `tests/unit/test_published_custody_receipts.py` | Preserve exact receipt/projection consistency, missing/changed original receipt rejection, indexed cold lookup, fresh creator/scoped-delete authority and interrupted/complete explicit upgrade survival. |
| `tests/unit/test_upload_terminal_receiving.py` and archive terminal registry/authority/completion/adjudication tests | Preserve original seal/writer lease and late-write/quiescence predicates. A new terminal state name cannot replace actual execution receiving. |
| Native `test_journal_result.py` and relevant Core summary consumers | Rebaseline fatal-on-missing/corrupt-derived-cache only to typed unavailability plus bounded coalesced revalidation. Preserve missing/foreign mandatory source/outcome/custody and incomplete semantic-result refusal, independent reader lifetime and last-reader receiving. |
| `tests/unit/test_publication_profile.py` | Preserve exact geometry/deadline validation, original-refusal propagation with both resource receivers, parent completion-receipt validation, and all three real independent-reader states. |

The old phase count or a test expecting a per-yield commit is not a safety argument. Replace such private traces with decisive counterexamples for the new boundaries. Keep canonical archival examples, member/history identities and framed journal checkpoint semantics unchanged. In particular, archival `append_checkpoint()` remains documentary evidence, not optional runtime recovery.

Add explicit refusal/fallback tests for: unresolved selected-candidate reply; known rejected candidate with continued old-base execution; count/storage/estimate refusal; completion coinciding with candidate eligibility; actual snapshot overrun; unclean accounting restart; cold derived-cache loss while a mandatory outcome remains; concurrent cold readers sharing one exact rebuild; and cancellation while original rebuild resources remain live.

For the index cut, additionally mutate existing content without changing row count; insert/delete content; change the selected root, worker epoch or current permission; and lose the final transaction reply. Cover all nine `_DATASET_TABLES`: snapshot, entry, assertion, profile, value, text chunk, edge, member and membership. Bulk/cascade cleanup and pending-generation replacement must either update the protected revision atomically or make the old build identity unusable. A successful retry proves complete canonical/index semantics against the current binding. Exact idempotent no-change writes need not create a new revision; rollback cannot leave a revision advanced without its rows or vice versa.

Capture one exact build/epoch and source/content revision at preparation start. Page with stable keys through bounded, source-consistent read snapshots; never adopt a newer revision halfway through the scan. Inject a mutation after the page's revision check but before its row read, and another after the last page but before acceptance. The final fresh acceptance transaction must reject both, including mutations to profile/value/text/edge rows that leave top-level counts equal. Reject revision wrap/reset, generation reuse and stale ORM identity-map values at acceptance. The `publication_input_revision` protects accepted semantic selection and identity in the same transaction as each change; exact immutable object receipts remain bound by the complete fixed manifest/root commitments. Store this source binding with the completed index seal, then recheck it at final visibility so a complete but stale generation cannot be published.

The final pointer, publication bit, exact original publication receipt and retirement marker are atomic; neither count equality nor an old digest permits acceptance. Inject index preparation/import/seal failure after archive sealing and prove exact sealed custody survives with publication still incomplete. Verify `_published_after` after upload retirement, reply loss and a later legitimate index rebuild; a matching collection ID, current active pointer or missing upload row alone is not proof of the original outcome. Hold a genuine original receiver across publication and prove maintenance continues to receive it while all forward admission/due paths refuse new work. Copy-intent work becomes eligible through the publication marker without unbounded final-transaction fan-out.

For the coupled public cut, interrupt body assembly before and after input transfer; pause original provider work while cancel/discard selects its terminal; lose selection/result replies; revoke permission; and restart due scheduling. Assert the same original owner remains charged/reachable, exact terminal readback uses fresh authorization, and no legacy synchronous fallback handles capacity or optional-policy refusal. A positive publication result and pending original settlement must remain distinguishable in API/client/CLI status. Shutdown must receive admitted input, finalizer and terminal actors through their existing lifecycle owners.

## 4. Shared policy, contracts and state cut

Use the #995 defaults unchanged: adaptive, 120 seconds net saved replay, fraction 1/8, unlimited successful optional creation count and two usable retained generations. The surviving `ArchiveWriteControlRecord` owns effective policy and bounded accounting outside disposable `construction_json`; `CollectionUploadRecord` retirement cannot lose cleanup liability. Policy is immutable for the logical construction unless an explicit safe update contract is implemented. SDK observations cannot override server resource/authority policy or mint independent allowance.

At a safe candidate, let `S` be completed useful replay avoided minus candidate restore/load/validation cost, `C` the incremental failure-free creation/publication/lifecycle cost, `U` credited fresh useful elapsed, and `O` actual optional charges plus distinct held reservations. Require `S >= 120s`, `C <= S/7` and `O+C <= U/7` simultaneously. Completion/mandatory anchors win; known remaining useful work `R <= C` favors finishing. Unknown is not zero. Repeated replay, idle waits and overlapping child durations do not create fresh credit. Future restore cost reduces `S` once, not also `C`; actual interruption/recovery work is reported separately. Preserve deterministic boundary tests, failed attempts, zero count/fraction, reserved count slots and overrun debt.

Native generation persistence precedes Core selection. Core checks exact source/construction/worker/grant and atomically selects the descriptor, settles successful count and adopts `resume_frontier_json`. Lost response is reconciled using the same grant/generation; no invented cross-database atomic transaction. Candidate dependencies stay pinned until original outcome receiving. Unknown cost/selection conservatively withholds further optional work without erasing confirmed archive outcomes. Mandatory semantic/currentness/receipt work continues in restart mode and under optional debt.

The hard cut must update SDK/Core/Native protocol fields, public status, API/client/CLI/configuration, generated contracts, model metadata and state baselines together. Processed progress is advisory and may regress after interruption; selected resumable progress is a precise dependency-consistent promise. No old per-microstep alias or parallel executor remains after cutover.

`riverhog_core.catalog_db`/`state_migrations`, `state-schema`, and the SDK prepared-primary application's existing schema owner govern initialization/upgrade. Test explicit upgrade/refusal on populated states and interrupted operations; startup verifies rather than repairs or erases. Preserve original construction IDs, prepared journal winners, captured source/history, request/crypto identity, intents, sealed outcomes and custody. Drain existing in-flight constructions through their owning old executable and retain that owner's state. Draining does not make an incompatible populated pre-v1 layout upgradeable: the selected default is to **refuse opening/upgrading that layout and leave it intact**, even if all constructions have finished. Initialize the new baseline only in new/empty state through explicit tooling. No export/import facility or lossless transformation is asserted by this reference; any separately authorized transfer needs its own concrete owner and preservation evidence.

Retain the maintained forward-upgrade process and data-survival/interrupted-upgrade tests for supported revisions. Include a populated same-revision/different-physical-schema counterexample: current `StateSchema.upgrade_connection()` verifies that state and must not silently replace it. Any ambiguous original blocks cutover and remains with the old owner/operator. There is no new-runtime old-codec reader, reset or replay-from-payload substitute. Quarantine/reclaim obsolete optional workspaces only after original worker/reader receiving. If required source or non-reconstructible state is unavailable, retain the explicit refusal/settlement path.

## 5. Actual unchanged-source F1 baseline

The maintained Python profiling owner completed on clean `c28eceaa2dfec1b3712135469ba12230e74f5ffa`, using this F1/C1/K2/Q1 workload:

```json
{
  "format": "riverhog-publication-workload/v1",
  "payload_sizes": [65536],
  "shared_root_members": 1,
  "shared_claim_members": 1,
  "retained_journals": 2,
  "required_journals": 1
}
```

| Actual observation | Result |
| --- | --- |
| Owned parent execution envelope | 64.411533234 seconds |
| Publication | 45.802746878 seconds |
| Fresh verification | 4.142022230 seconds; 58 actual stored reads; 167,370 stored octets |
| Warm verification | 4.191056257 seconds; 41 actual stored reads; 135,013 stored octets |
| Evicted verification | 4.161700759 seconds; 58 actual stored reads; 167,370 stored octets |
| Complete retained output | 1 member, 65,536 payload bytes, 4 physical journals, 5 entries, 19,873 journal bytes |
| Bound/required history | 3 journals, 4 entries, 17,966 journal bytes |
| Stored commitments per pass | 21 objects, 20 whole-object checks, 1 part check; identical root across all readers |
| Receiving and source | Child exit 0; child and workspace cleanup received; source unchanged and clean; source/upload scratch removed before verification |

The unchanged [original profile report](evidence/F1_BASELINE.json), [final phase record](evidence/F1_BASELINE_PHASE.json) and [workload document](evidence/F1_WORKLOAD.json) are included in this handoff. Run ID: `48fb45d2-0e13-4915-b65b-e72020aab459`. The phase file records final `passed`; it overwrites earlier phases and is not a full event log.

Conditions are authenticated in-process TestClient HTTP, SQLite 3.45.1, encrypted in-memory archive storage, archive scrypt work factor 1, producer cache disabled, one-member-at-a-time append, bounded coordinator polls at 0.05 seconds, and default 900-second finalization deadline. Fresh/evicted states replace the Native reader object; warm state reuses immutable verified-byte cache. OS caches are not cleared. The report explicitly disclaims production memory qualification and independent installed recovery. [Maintained fixture](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/tests/support/qualification/publication_profile.py).

This is one successful baseline, not a matched comparison, speedup, new executor test or proof of zero optional checkpoints. It reports actual stored-byte calls and full verification, not DB transaction/physical synchronization counts. The #991 historical counters are separate evidence and must not be relabelled as this run's observations.

The focused SDK experiment uses the real POSIX prepared store and Native evidence with an in-process API fixture and guarded single-threaded read-lease instrumentation. Its fresh control-load barrier at entry is measured comparison setup, not the proposed status path. Its fixture is not authenticated HTTP; the separate unchanged HTTP retry witness in §6 supplies that narrower existing-path evidence. The Core experiment uses the real encrypted filesystem upload/admission/checkpoint owners and compares controlled individual report settlement with the already existing atomic report batch. It does not implement the proposed conductor or public activation. Neither experiment establishes cross-member SDK atomicity, new verified-custody batching, Windows/SQLite lease lifetime, physical synchronization counts, installed adapter behavior or new-path F1 qualification. Actual executions and raw counters are reported separately in VALIDATION.md.

## 6. Reproducible environment and invocation

The separate task-local environment is `/workspace/scratch/939b84c043b0/core-sdk-997-env`. It uses pinned Python 3.12.3 and uv 0.11.24, with 70 locked Core/SDK/Native-test packages plus 31 locked root dev-group packages. The #996 environment was not changed. The unchanged lock SHA-256 is `16b64145aa11bfbd6d2076a7a5adc783ed7d2e000237f9c31e9977ef48815f24`.

Run from the desired exact-source checkout, without another agent using/resyncing the environment:

```bash
mkdir -p /workspace/scratch/939b84c043b0/997-qualification-tmp

UV_CACHE_DIR=/workspace/scratch/939b84c043b0/toolchain/cache \
UV_PROJECT_ENVIRONMENT=/workspace/scratch/939b84c043b0/core-sdk-997-env \
TMPDIR=/workspace/scratch/939b84c043b0/997-qualification-tmp \
/workspace/scratch/939b84c043b0/toolchain/uv-0.11.24/bin/uv sync \
  --locked --package riverhog-server --package riverhog-client \
  --package riverhog-provenance --extra test --no-default-groups \
  --python /workspace/scratch/939b84c043b0/toolchain/python/cpython-3.12.3-linux-x86_64-gnu/bin/python3

UV_CACHE_DIR=/workspace/scratch/939b84c043b0/toolchain/cache \
UV_PROJECT_ENVIRONMENT=/workspace/scratch/939b84c043b0/core-sdk-997-env \
TMPDIR=/workspace/scratch/939b84c043b0/997-qualification-tmp \
/workspace/scratch/939b84c043b0/toolchain/uv-0.11.24/bin/uv sync \
  --locked --only-group dev --inexact \
  --python /workspace/scratch/939b84c043b0/toolchain/python/cpython-3.12.3-linux-x86_64-gnu/bin/python3
```

The second command keeps selected Core/SDK packages while adding root-owned testing dependencies, including httpx2. It avoids a whole-workspace application install. Observed pytest 9.1.1, httpx2 2.12.0, FastAPI 0.141.1, Starlette 1.3.1, cryptography 50.0.0 and SQLAlchemy 2.0.51; retained-resource's native extension built successfully. Use external TMPDIR and explicit external pytest `--basetemp`; the system temporary directory is unavailable in this task environment, so implicit fallback can dirty the checkout.

The normal maintained entrypoint is:

```bash
make profile args='--scenario collection-publication --publication-workload /absolute/997-f1-workload.json --output /absolute/997-f1-profile'
```

The profile requires a completely clean checkout, including untracked files, and verifies exact source before and after. Run the unchanged baseline from a separate clean checkout of `c28eceaa2dfec1b3712135469ba12230e74f5ffa`, with workload/output outside it. The published reference adds `.reference` files and therefore has a different commit identity even though product paths match that base. A run on its clean published tree must record that actual reference SHA; it is not another `c28eceaa…` execution. Never bypass the source check or claim a dirty in-progress reference as exact-source evidence.

It was attempted here and could not start: `mise` is unavailable, wrapper error 127 / Make exit 2. The successful focused fallback called the unchanged Python owner with the existing source-owned numeric resource profile:

```bash
TMPDIR=/workspace/scratch/939b84c043b0/997-qualification-tmp \
RIVERHOG_PROVENANCE_STATE_HOME=/workspace/scratch/939b84c043b0/997-qualification-tmp/provenance-state \
RIVERHOG_CONFIG=/workspace/scratch/939b84c043b0/997-qualification-tmp/unavailable-deployed-config \
/workspace/scratch/939b84c043b0/core-sdk-997-env/bin/python -c \
  'import os; from scripts.validation_numeric import bind_numeric_environment, qualification_numeric_policy; bind_numeric_environment(os.environ, qualification_numeric_policy()); from scripts.transfer_profile import main; raise SystemExit(main())' \
  --scenario collection-publication \
  --publication-workload /workspace/scratch/939b84c043b0/997-f1-workload.json \
  --output /workspace/scratch/939b84c043b0/997-f1-profile
```

The command reads `scripts/qualification_numeric.json` and binds the checked implementation identity through its existing helper. The observed profile is `qualification`, max_work `907920445364`, max_integer_bits `84`, numeric reader SHA-256 `4f08bc6148007c7194b2451cdd885b04ada30806b6ee1f6c5e6d29433885720a`. No values were invented or raised. The maintained parent/child source checks, exact completion receipt, original child receiving and scratch cleanup all ran. This fallback is not a passing Make envelope, required growth collector or full qualification gate. No source/test modification or patched skip was used.

### Focused unchanged safety selection

The following nine existing witnesses are the concrete small safety baseline to retain while changing the relevant owners. They passed here with zero failures/errors/skips in **54.520288268 seconds** of recorded pytest time (JUnit suite time 54.496 seconds). The actual HTTP prepared-primary retry took 29.18 seconds; the confirmed-but-running original-worker witness took 14.10 seconds. The remaining cases cover original crypto, dispatch/outcome, late seals, actual Native receiving, independent progress and close refusal. No new tests or product patches were installed for this run.

The first attempt was blocked during collection because the selected environment intentionally omitted the filesystem-adapter application: two import errors, exit 4, recorder time 7.735645617 seconds. The retry supplied the two unchanged checkout source roots below; no environment resync, warning suppression or patched skip was used. This is actual source-path fixture evidence, not installed adapter qualification. Both attempts and their JUnit/timing records remain reported in VALIDATION.md.

```bash
TMPDIR=/workspace/scratch/939b84c043b0/997-qualification-tmp \
PYTHONPATH=/workspace/scratch/939b84c043b0/riverhog-997/some-implementations/riverhog/storage/filesystem/src:/workspace/scratch/939b84c043b0/riverhog-997/packages/riverhog-storage-adapter-asgi-support/src \
RIVERHOG_CI_TIMING_DIR=/workspace/scratch/939b84c043b0/997-safety-timing \
/workspace/scratch/939b84c043b0/core-sdk-997-env/bin/python -c \
  'import os,sys; from scripts.validation_numeric import bind_numeric_environment, qualification_numeric_policy; bind_numeric_environment(os.environ, qualification_numeric_policy()); os.execv(sys.executable,[sys.executable,"-m","pytest",*sys.argv[1:]])' \
  -q -p scripts.ci_timing -o junit_family=xunit1 \
  --basetemp=/workspace/scratch/939b84c043b0/997-safety-adapter-pytest \
  --junitxml=/workspace/scratch/939b84c043b0/997-safety-adapter.xml --durations=15 \
  tests/unit/test_prepared_primary_publication.py::test_durable_primary_replays_after_actual_http_stage_bind_and_decision_acknowledgement_loss \
  tests/unit/test_archive_write_admission.py::test_original_crypto_is_committed_before_provider_begin \
  tests/unit/test_archive_write_admission.py::test_running_dispatch_keeps_its_reservation_until_actual_return \
  tests/unit/test_archive_write_admission.py::test_returned_result_and_catalog_change_commit_together_and_retry_exactly \
  tests/unit/test_archive_upload_terminal_adjudication.py::test_positive_late_seal_refuses_cancel_without_erasing_original_state \
  tests/unit/test_archive_upload_terminal_completion.py::test_confirmed_database_state_cannot_release_a_still_running_original_worker \
  tests/unit/test_journal_retirement_registry.py::test_actual_native_extent_receives_before_withdrawing_ordinary_charge \
  tests/unit/test_journal_retirement_registry.py::test_stalled_original_file_io_preserves_custody_and_allows_another_workspace \
  tests/unit/test_journal_retirement_registry.py::test_native_close_refusal_keeps_original_lease_and_retries_only_receiving
```

Product and test paths remained unchanged at `c28eceaa…`; these focused runs occurred while `.reference` was being authored, and the recorder truthfully reports `source_clean=false`. They are separate from the clean F1 evidence and do not satisfy an exact-final-source/merge-checkout gate. No growth witnesses were selected, so #989's complete coordination/worker/receiving growth proof remains in its maintained broader gates. `make lint` was also attempted and stopped at missing `mise` (Make exit 2, wrapper error 127); lint did not execute.

## 7. Comparative evidence and full qualification

Record matched old, redesigned-restart and redesigned-adaptive workloads with the same captured source/prepared evidence, selection/configuration and authentication/encryption conditions. Preserve the captured nondeterministic winner when comparing semantic identity; independent fresh ciphertext is not expected to be byte-identical. Compare adaptive optional tax to redesigned restart and structural cost to old execution. Include failures, denied/overrun candidates, real interruption, replay time and receiving tails. No actual speedup follows from reduced checkpoint counters or one fixture.

Observe actual SDK preparation/append/history/finalization/receiving, Native source/semantic work, parent/child codec bytes, Core adoption/writer ownership, attempted SQL/transactions, required effect barriers, optional generation selections, provider calls, ciphertext bytes and cleanup. Keep each counter's owner/meaning explicit. Python `os.fsync` wrappers do not observe SQLite's VFS syncs; report physical synchronization unmeasured unless the actual primitive is instrumented. Source-owned growth declarations/witnesses must cover the full execution/capture/restore/reclamation lifecycle, not a convenient caller facade or reduced final count.

| Qualification surface | Required evidence |
| --- | --- |
| SQLite and PostgreSQL Core catalogs | Actual grant/frontier/count transactions, mandatory batch/result atomicity, lost commit/reply reconciliation, concurrent/stale workers, current authorization and explicit populated-schema transitions. No provider wait inside catalog writers. |
| Linux | Built/installed Native and SDK exact file/directory/SQLite identity, private placement, partial constructor, actual process exit/join, interrupted receiving, source mutation and complete lifetime growth. |
| macOS | Equivalent installed paths and actual supported VFS/sync/owner behavior; Linux results do not qualify Darwin durability. |
| Windows | Native ACL/file identity, sharing/rename/delete/sidecar behavior, original handle receiving and installed wheels. POSIX mode checks are not Windows privacy evidence. |
| Independent reads/recovery | Complete fresh/warm/evicted verification plus separate installed archive/key-only recovery; the in-process fixture does not satisfy the latter. |
| State/configuration/protocol consumers | Coherent hard cut across SDK/Core/Native, API/client/CLI and generated contracts, explicit old in-flight disposition and incompatible startup refusal. |

Use focused original-owner tests and `make lint` while integrating. Retain the normal complete `make lint`, `make unit`, `make dist-smoke`, `make build` rails; use `make linux-qualification` for complete local Linux qualification when appropriate. Native/platform receiving and staged installation run through `make client-platform-qualification` on each supported host. PostgreSQL/database/concurrency and filesystem gates remain with their maintained owners. Preserve #993's recorder/actual-primitive correction; missing or skipped required witness output is not a pass.

Before integration handoff/merge, require the exact final source and its tested merge checkout to pass all applicable required checks, with original runs stopped and evidence received. Report blocked scopes explicitly. This reference neither completes #997/#994 nor authorizes provider qualification, release, deployment, archive-format changes or resumption of independent #991 work.
