# Source-grounded cost map for #991

Audited source: `8ce49de94c32ae39562aab5ea4178b1dc8c75e77` on `codex/964-session-owner` / PR #979. This is static source research, not a timing profile or composed growth proof. This chapter is a static source audit; actual diagnostic execution is reported separately in [RESULTS.md](RESULTS.md). Product code is unchanged. The inspected publication/producer/history/collection-upload source paths below have no diff from the #991 diagnostic source `2eb7e48f7c9c648f58e27dd0ae4dd11d0cf54457`; broader source equality is not claimed.

## Reading anchors

Source links below are pinned to the audited commit. Unlinked symbols name operations in the corresponding owner.

- **Fixture owner**: [tests/support/qualification/canonical_history_scale.py](https://github.com/nashspence/riverhog/blob/8ce49de94c32ae39562aab5ea4178b1dc8c75e77/tests/support/qualification/canonical_history_scale.py) `publish_shared_history`.
- **Profile owner**: [tests/support/qualification/publication_profile.py](https://github.com/nashspence/riverhog/blob/8ce49de94c32ae39562aab5ea4178b1dc8c75e77/tests/support/qualification/publication_profile.py) `run_publication_profile`.
- **Exhaustive observer**: [tests/support/qualification/publication_evidence.py](https://github.com/nashspence/riverhog/blob/8ce49de94c32ae39562aab5ea4178b1dc8c75e77/tests/support/qualification/publication_evidence.py) `measure_published_history` and `verify_stored_publication`.
- **Client owner**: [packages/riverhog-client/src/riverhog_client/producer.py](https://github.com/nashspence/riverhog/blob/8ce49de94c32ae39562aab5ea4178b1dc8c75e77/packages/riverhog-client/src/riverhog_client/producer.py) `IncrementalCollectionProducer`.
- **Prepared primary**: [packages/riverhog-client/src/riverhog_client/_prepared_primary_publisher.py](https://github.com/nashspence/riverhog/blob/8ce49de94c32ae39562aab5ea4178b1dc8c75e77/packages/riverhog-client/src/riverhog_client/_prepared_primary_publisher.py) `PrimaryPublisher`; `_prepared_primary_store.py::PrimaryStore`; `_prepared_primary_posix.py`.
- **Upload owner**: [riverhog/src/riverhog_core/services/collection_uploads.py](https://github.com/nashspence/riverhog/blob/8ce49de94c32ae39562aab5ea4178b1dc8c75e77/riverhog/src/riverhog_core/services/collection_uploads.py) `SqlAlchemyCollectionUploadService`.
- **Native journal**: [packages/riverhog-provenance/src/riverhog_provenance/journal.py](https://github.com/nashspence/riverhog/blob/8ce49de94c32ae39562aab5ea4178b1dc8c75e77/packages/riverhog-provenance/src/riverhog_provenance/journal.py) `validate_journal_chunks`; `journal_session.py::JournalValidationSession`; `_session_checkpoint.py::ValidationLedger`; `_journal_prefix.py::JournalPrefix`.
- **History closure**: [packages/riverhog-provenance/src/riverhog_provenance/history.py](https://github.com/nashspence/riverhog/blob/8ce49de94c32ae39562aab5ea4178b1dc8c75e77/packages/riverhog-provenance/src/riverhog_provenance/history.py) `MemberHistoryClosure`.
- **Published reads**: [riverhog/src/riverhog_core/canonical_provenance_archive.py](https://github.com/nashspence/riverhog/blob/8ce49de94c32ae39562aab5ea4178b1dc8c75e77/riverhog/src/riverhog_core/canonical_provenance_archive.py) `PublishedCanonicalProvenance`; `provenance_archive_read.py::CanonicalProvenanceArchiveReader`.
- **Local transport/worker driver**: [tests/unit/test_operation_lifecycle_api.py](https://github.com/nashspence/riverhog/blob/8ce49de94c32ae39562aab5ea4178b1dc8c75e77/tests/unit/test_operation_lifecycle_api.py) `_container`, `_LocalApi`, `_api`; [tests/support/uploaded_archive.py](https://github.com/nashspence/riverhog/blob/8ce49de94c32ae39562aab5ea4178b1dc8c75e77/tests/support/uploaded_archive.py) `UploadedArchiveStore`.

## Important findings with attribution confidence

### 1. The observer has a source-explainable ciphertext-read floor

In `verify_stored_publication`, every archive object is first read with `store.read_stored`. Multipart objects decrypt/check that read. Whole objects additionally call `store.iter_archive_object`, whose `UploadedArchiveStore` implementation reads the same ciphertext again before digest/decryption checks. Thus this observer block accounts for exactly

`2 * whole_object_checks + multipart_object_count`

stored reads per complete pass. Do not substitute `part_checks` for multipart object count when one object has multiple parts.

The #990 16-member receipt has 126 archive objects, 110 whole-object checks and 16 multipart objects: 236 reads. This equals the reported 236-read warm pass total. Hence a large part of the stored-read count is an explicit exhaustive-observer cost, and verified-plaintext cache reuse cannot eliminate it. The 1/2/4-member warm counts 41/54/80 fit `13*N+28`, but their object geometry must be checked before asserting the same decomposition from timing summaries alone.

**Confidence:** exact source mechanism and arithmetic for the published 16-member receipt. **Disposition:** required current observer checks; the second read may be a candidate for a semantics-preserving observer refactor only after preserving independent commitment, decryption, and corruption checks. It does not explain publication time, which excludes this observer.

### 2. Native validation can recur even when ciphertext reads disappear

Each `measure_published_history` pass:

1. Checks the archive root and every stored archive object.
2. Constructs/prepares a reader, receives all exact member bindings and all physical journal IDs.
3. Validates every physical journal from bytes.
4. Constructs one `MemberHistoryClosure` for shared documentary dependencies.
5. Constructs one new closure per member for bound history, then one new closure per member for retained history, resolves it, and traverses each selected summary's frames.

Therefore the explicit observer creates `2*N+1` closure owners per reader pass, or `6*N+3` over the three required reader states. Physical journal validation is separately repeated three times. These are static outer-loop multiplicities, not total inner SQL/validation counts. `MemberHistoryClosure` creates its own temporary SQLite worklist and owns a bounded summary cache. A fresh per-member closure cannot reuse another closure's Native validation.

`summary_at` intentionally rereads and hashes authenticated provider bytes even on a summary-cache hit so current read fences still run. The bounded cache can reuse semantic validation only within that exact owner, while misses create a new `JournalValidationSession`. `PublishedCanonicalProvenance._read_object` also performs an object-identity catalog snapshot before checking its verified-plaintext cache. Warm ciphertext counts therefore cannot prove warm Native CPU/SQL work or a whole-path growth bound.

**Confidence:** exact source loops and lifetime boundaries. **Disposition:** current exhaustive test-observer work and deliberate current-fence enforcement, with potential reuse questions requiring exact immutable identity, current authority, cache admission and resource receiving. Do not remove verification or read fences to improve the benchmark.

### 3. Server finalization revalidates selected shared roots per attached member

`SqlAlchemyCollectionUploadService._validate_provenance_closure` scans a bounded member batch. For each member it validates the primary journal prefix, verifies the exact primary/history/materialization relationship, then validates every selected nonprimary root directly from bytes. The same selected shared root therefore receives a separate validation call for each attached member in this phase.

After all member batches, `_validate_staged_canonical_journal_set` separately resolves retained member closures in one accumulated closure, checks that every sealed physical journal belongs to that closure, validates every physical journal, adds each result to a corpus and validates the corpus. These checks have different purposes; repeated bytes are not automatically redundant.

**Confidence:** exact call sites and loop multiplicity. **Cost hypothesis:** shared-root validation bytes can include a term proportional to attachment fanout times selected shared-prefix bytes. If the shared root also grows with documentary claims, growing both dimensions together can hide a product term. This is not by itself a proven growth-contract violation: the owner must reconcile required logical history versus unique physical history and its declared cost. Counter sets should distinguish exact `(journal, prefix, catalog pin, accepted authority)`, caller and phase so valid repeated safety checks do not get conflated with duplicate semantic work.

### 4. Per-member append comprises much more than payload upload

`append_inputs` hashes/verifies source bytes, creates candidate sources, prepares the primary, registers identities, records raw-part digests, replays the retained primary and accepted materialization decision, uploads available units, and performs a bounded pending-custody reconciliation. `PrimaryPublisher.prepare` compares exact retry descriptors; on first authorship it checks the session and member absence, obtains the observation and builds the member journal. `replay` retains the winner, reads authoritative remote state, uploads/seals/binds the journal if absent, and records the accepted decision. Accepted decisions short-circuit future replay.

The fixture has one append call per member. It does not exercise a large batch of members. The fixture uses a `PrimaryStore` with the default `posix_primary_files` factory: prepared-primary file custody is POSIX here, not the separate prepared-primary SQLite backend. Catalog SQL, admitted Native SQL and disposable ordinary SQLite validation/worklists remain distinct. The last category does not pass through `sqlite_admission` cursor hooks. POSIX control writes include fsync/link and current-owner checks. Attribute those operations before drawing a “SQLite dominated append” conclusion.

**Confidence:** source call graph. **Cost:** unmeasured by this audit. Required exact preparation/replay is distinct from unnecessarily repeated preparation; count new authorships, accepted-decision fast paths, storage/control operations and API round trips per input.

## Lifecycle accounting map

| Phase | Owning operations / caller | Purpose and dimensions to record | Multiplicity / attribution boundary |
| --- | --- | --- | --- |
| Setup | Profile → fixture → `_container`, app keys, source creation/hash, `PrimaryStore`, producer constructor / `open_primary_session` | Catalog/schema preparation, auth bootstrap, exact source bytes, stable preparation ownership; members, payload bytes, catalog baseline population, prepared storage allocation | Setup precedes the fixture's first `append-inputs` event; initial key/runtime/client creation is observer construction cost. Record its own wall/CPU and storage operations. |
| Append | Fixture member loop → producer `append_inputs`, `_append_sources`, `_upload_available`; `PrimaryPublisher.prepare/replay`; service registration/journal/upload paths | Preserve exact payload/source identity, authored provenance and replay winner; N, bytes/chunks/parts, Native assertions/frames, primary metadata bytes, control-record writes, existing catalog rows | One fixture append/member. Hashing, authorship, API/auth/catalog work, transport setup, upload workers and rechecks need distinct subowners; parent time includes nested calls. |
| Primary-history receive | Fixture loop → binding API + streamed journal API → `validate_journal` and frame walk | Build the exact documentary fixture geometry; N primary journal bytes and selected prefix extents, C documentary claims | Re-downloads and revalidates every primary specifically to author the test's shared-history geometry. Count this as fixture preparation, not a generic production append cost. |
| Supplemental/shared history preparation | Fixture → `create_journal`, `validate_journal`, upload/seal K supplemental journals + one shared journal | Retained K, required Q, documentary C, claims/bytes/frame widths; build exact selected roots | K+1 fixture journals; construction itself can validate Native content. Record authored bytes/frames and separate API admission validation. |
| History staging | Fixture member loop → `MemberHistoryBuilder`, page/descriptor staging, `bind_member_histories` | Exact immutable member histories, root attachments F, imports, page/descriptor bytes, repeated identical root page identities | One member builder/binding per member, pages by authority. Empty/shared structures can recur; count requests and distinct identities. |
| Local finish/replay | Producer `finish` → `PrimaryStore.recover_step`, inventory/freeze, `advance_closing`, `_settle_primary` | Explicit completion and original preparation, inventory members and control bytes, unsettled records, cold/retry state | Separate normal completed fast paths from recovery after failure. Bound iterations and actual progress; measure full lifecycle including resumed attempts. |
| Coordinator / status | `finish` polling → `_LocalApi.get_collection_upload_session` → due custody/finalization processing → authenticated HTTP status | Drive real work in this fixture, current auth and status, original workers; polls, returned work, empty polls, queue/turn limits | The local test adapter drives worker services before the HTTP status request; status wall time can include scheduler work. It sleeps at least 0.05 s. Heartbeat waits are not CPU use. |
| Finalization / custody | Upload `_finalization_owner` → retained `ArchiveFinalizationPipeline` stages | Native seal/report receiving, member history, closure validation, structural/archive metadata and volume authority, artifact tree, per-member custody, initial tags/description, catalog projection | Sequential stage dependencies plus retained worker turns. Record stage identities, actual submissions/starts/joins, SQL txn windows, bytes, journals, roots, objects, selected members and retry counts; do not sum overlapping worker time into elapsed time. |
| Catalog projection / discovery | `_advance_catalog_projection` → cursor-driven identity/tags/artifacts/journals/bindings/segments/archive objects plus `_advance_catalog_canonical_index` | Rebuildable indexed projections; unique physical history vs selected snapshots, nodes/edges, members, result population | Follow delegates through canonical-discovery growth declarations; owner-local bounds do not establish caller invocation counts. |
| Verification | Profile three-pass loop → `measure_published_history` → stored checks + reader preparation + Native journal/history closures | Complete identities, custody, ciphertext commitments, physical and logical history, semantic validity after scratch removal | Separate mandatory test observer work from production publication; preserve each required pass. Static outer-loop counts above guide targeted instrumentation. |
| Cleanup | Producer `stop` / exact heartbeat join, primary store close, summary/readers close, API and container close; profile parent receives helper then removes workspace | Retain original owners through uncertain close/exit; resources, active workers, pending storage/workspaces, failures | Profile's successful observed result is insufficient until original process and workspace receiving receipts exist. Count cleanup CPU/blocking, retries and unreceived owners, not just close requests. |

## Growth evidence that helps, and what it does not establish

- [packages/riverhog-provenance/tests/test_validation_checkpoint_growth.py](https://github.com/nashspence/riverhog/blob/8ce49de94c32ae39562aab5ea4178b1dc8c75e77/packages/riverhog-provenance/tests/test_validation_checkpoint_growth.py) binds to `ValidationLedger` declarations and observes cold SQLite work, rows/metadata widths and actual effects across histories including 0/1/17/129/1025 frames, wide control metadata and interrupted/rejected transitions. It supports checkpoint-local behavior, not end-to-end invocation counts.
- `journal_session.py` declares `advance`, `progress`, `result`, construction/open-retained and close with delegates. Related `test_journal_session_*growth.py`, `test_entry_replay_*growth.py`, `test_source_*growth.py`, `test_history_pending_growth.py` should be consumed at the owning operation. One default journal advance has a finite work budget; `validate_journal_chunks` synchronously loops until completion, so the whole-call cost depends on total transitions and repeated construction.
- [tests/unit/test_upload_journal_service_coordination_growth.py](https://github.com/nashspence/riverhog/blob/8ce49de94c32ae39562aab5ea4178b1dc8c75e77/tests/unit/test_upload_journal_service_coordination_growth.py), `test_finalization_readiness_growth.py`, and `test_canonical_discovery_*growth.py` cover particular service/caller/delegate units. Retain their exact primitive definitions and cold/unrelated-population dimensions when composing observations.
- [tests/unit/test_publication_history_geometry.py](https://github.com/nashspence/riverhog/blob/8ce49de94c32ae39562aab5ea4178b1dc8c75e77/tests/unit/test_publication_history_geometry.py) proves real exact history extents, member scope, custody and ciphertext corruption/recovery for small independent F/K/Q/C configurations. It records output and reads, not a complete composed CPU/SQL/work bound.
- [tests/unit/test_member_history_finalization.py](https://github.com/nashspence/riverhog/blob/8ce49de94c32ae39562aab5ea4178b1dc8c75e77/tests/unit/test_member_history_finalization.py) checks resumed encrypted structure and stable primary semantics. [packages/riverhog-provenance/tests/test_history.py](https://github.com/nashspence/riverhog/blob/8ce49de94c32ae39562aab5ea4178b1dc8c75e77/packages/riverhog-provenance/tests/test_history.py) explicitly tests fresh reads/current fences while reusing validation, changed bytes/anchors, above-cache-size acceptance and documentary-reference reuse. These tests constrain proposed reuse.
- This static search found growth decorators on `MemberHistoryClosure._pending_history/_pending_snapshot`, not a complete `resolve/summary_at` caller-composition proof. The absence of decorators in those local methods is an audit lead, not proof that all repository growth coverage is absent.

## Recommended focused accounting questions

1. Reconcile measured phase totals first: where are main CPU, worker CPU, request/queue/lock waits, fsync/storage time and unassigned residual? Preserve inclusive and exclusive scopes.
2. Record Native journal sessions opened, source bytes admitted, frames, advance transitions, checkpoint loads/fences/publishes, cold SQL statements/VM instructions, and result-reader/workspace opens/closes. Group by caller and exact selected-prefix identity. Separate catalog SQL from Native SQL.
3. For shared-root finalization, hold N/payload fixed and vary F then C (including roots with different byte/frame width). Determine whether per-member validations materially cost time and whether existing accepted proofs can be reused without weakening current authority/custody.
4. For observer verification, expose phase subspans and the read floor above. Attribute per-pass new closure and journal sessions. Any observer efficiency change must preserve all exact, independent verification outcomes.
5. For append, count durable prepared-primary operations and authenticated request/transport overhead alongside Native work. Do not select an optimization merely from stack-sample frequency.
6. Maintain separate correctness, growth-conformance and timing dispositions. No estimate here is permission to start 1,000/10,000/100,000 trials, replace exact tests, remove security checks, or introduce numerical speed gates.

## Issue evidence referenced

- Current #991 scope and cost-aware sequencing: https://github.com/nashspence/riverhog/issues/991
- Serial 1/2/4 diagnostic observations and invalid thread-entry attempt: https://github.com/nashspence/riverhog/issues/991#issuecomment-6853257215
- Current accepted source checkpoint: https://github.com/nashspence/riverhog/issues/964#issuecomment-6854178345
- Prior exact 16-member validation and execution conditions: https://github.com/nashspence/riverhog/blob/2eb7e48f7c9c648f58e27dd0ae4dd11d0cf54457/.reference/VALIDATION.md

The #991 small timings are issue-reported evidence here; their raw sampler records have not been inspected by this static task. No measured share, causality, regression ratio, projected completion time, or whole-path complexity is inferred from them.
