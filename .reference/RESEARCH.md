# Primary sources and audited implementation

Audited implementation: `533ac4ee06def4692288c39c9279bf9387525a3e`, the exact
checkpoint named in [#984](https://github.com/nashspence/riverhog/issues/984).
Additional drift inspection: `03dfc3c433372e5a965d37bbc5faf336feb05f17`, the
integration tip at read time. The latter's 30-file delta includes cleanup identity
factoring, snapshot changes and fixture corrections; it is not the model's product
base and is not implicitly qualified by this packet. No product execution is
claimed for either SHA. The reference model uses Python/SQLite independently.

## Source owners

All links below are pinned to the issue's base. Symbols, not this document, own
product behavior. The integration agent must reconcile the current source.

| Source | Finding relevant to the selected design |
| --- | --- |
| [collection_uploads.py](https://github.com/nashspence/riverhog/blob/533ac4ee06def4692288c39c9279bf9387525a3e/riverhog/src/riverhog_core/services/collection_uploads.py): `upload_unit`, `cancel`, `discard_orphan` | Admission state is checked before leaving a transaction; provider calls follow outside it. Terminal paths collect known checkpoints; an uncheckpointed future begin is absent. |
| Same owner: `_record_sealed_volume`, `_record_payload_custody_progress` | Late exact receipts affect volume/custody state. Their truthful evidence must be retained, not discarded merely because terminal selection rotated an epoch. |
| Same owner: `_orphan_discard_plan`, `require_discard_access`, `requeue_interrupted_orphan_discards_for_startup` | Orphan/deletion/live-claim guards and original plan terms must remain. Startup currently restores discarding to orphaned, not a retained exact terminal operation. |
| [raw_upload.py](https://github.com/nashspence/riverhog/blob/533ac4ee06def4692288c39c9279bf9387525a3e/riverhog/src/riverhog_core/raw_upload.py): `RawVolumeUploader.open`, `_complete`, `abort`, `merge_raw_upload_checkpoints` | Crypto/preparation exists before begin but its durable checkpoint is saved after begin. Completion/abort and checkpoint deletion have independent uncertainty windows. |
| [pack_upload.py](https://github.com/nashspence/riverhog/blob/533ac4ee06def4692288c39c9279bf9387525a3e/riverhog/src/riverhog_core/pack_upload.py): `PackVolumeUploader.open`, `abort` | The same ownership cut is required for pack publication, not a raw-only workaround. |
| [sqlalchemy_archive_upload_checkpoints.py](https://github.com/nashspence/riverhog/blob/533ac4ee06def4692288c39c9279bf9387525a3e/riverhog/src/riverhog_core/stores/sqlalchemy_archive_upload_checkpoints.py): `_merge` | Currently locks volume before upload; the proposed stable order requires coordinated reversal. Progress merging is not execution quiescence. |
| [collection_registration_cleanup.py](https://github.com/nashspence/riverhog/blob/533ac4ee06def4692288c39c9279bf9387525a3e/riverhog/src/riverhog_core/services/collection_registration_cleanup.py): `fence_registration_disposal`, `advance_registration_cleanup` | Already owns fenced bounded hidden cleanup. Defer destructive cleaning until provider drainage and terminal-right adjudication. |
| [collection_workflows.py](https://github.com/nashspence/riverhog/blob/533ac4ee06def4692288c39c9279bf9387525a3e/riverhog/src/riverhog_core/services/collection_workflows.py): `renew_claim`, `restart_claim` | Expired claims can renew when output exists; terminal selection needs a coupled live-claim interlock, not just a stale check. |
| [collection_deletions.py](https://github.com/nashspence/riverhog/blob/533ac4ee06def4692288c39c9279bf9387525a3e/riverhog/src/riverhog_core/services/collection_deletions.py): `delete` | Existing selected-deletion retention is an owner-local pattern, not permission to reuse collection deletion authority for upload cancel. |
| [app.py](https://github.com/nashspence/riverhog/blob/533ac4ee06def4692288c39c9279bf9387525a3e/riverhog/src/riverhog_api/app.py): `_process_archive_maintenance`, `_run_archive_upload_reaper` | Maintenance uses a shared operation lock; provider waits cannot be added to its coordination turn. |
| [mirrored_archive_resumable_object_store.py](https://github.com/nashspence/riverhog/blob/533ac4ee06def4692288c39c9279bf9387525a3e/riverhog/src/riverhog_core/stores/mirrored_archive_resumable_object_store.py) | Begin/parts/complete and completed lookup can touch optional cache. Track child ownership and preserve existing abandoning cleanup. |
| [storage_adapter_archive_store.py](https://github.com/nashspence/riverhog/blob/533ac4ee06def4692288c39c9279bf9387525a3e/riverhog/src/riverhog_core/stores/storage_adapter_archive_store.py): `discard_collection_archive_upload` | Calls unbounded prefix deletion. A bounded terminal provider-step contract is necessary. |
| [protocol.py](https://github.com/nashspence/riverhog/blob/533ac4ee06def4692288c39c9279bf9387525a3e/packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/protocol.py): `StorageAdapterPort` | Durable sessions and validated receipts exist; explicit whole-call closure/unknown and bounded terminal disposal need the scoped current-only operational cut. |
| [S3 adapter](https://github.com/nashspence/riverhog/blob/533ac4ee06def4692288c39c9279bf9387525a3e/some-implementations/riverhog/storage/s3-support/src/a_riverhog_s3_store_lib/adapter.py): `begin_write`, `_active_write_for_key`, `abort_write`, `_delete_prefix_all_versions` | Begin can create after its single active-upload lookup. Abort returns without proving all in-flight requests stopped. Prefix cleanup internally paginates and retains a growing deleted set. |
| [filesystem adapter](https://github.com/nashspence/riverhog/blob/533ac4ee06def4692288c39c9279bf9387525a3e/some-implementations/riverhog/storage/filesystem/src/a_riverhog_filesystem_store/adapter.py): `FilesystemStorageAdapter` | Instance lifetime ownership and per-object gates are implementation facts, not a generic remote-request cancellation guarantee. |
| [support client](https://github.com/nashspence/riverhog/blob/533ac4ee06def4692288c39c9279bf9387525a3e/packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/client.py) | Adapter RPC response loss is another uncertainty boundary; correlation must survive both HTTP hops. |

## External primary sources (reviewed 2026-11-01)

1. [AWS AbortMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html)
   explicitly discusses late in-progress parts and verification with ListParts.
   Deduction: an abort response alone is insufficient for the requested drain.
2. [AWS CreateMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html)
   defines provider-created UploadId and its request fields. No whole-call client
   idempotency/fencing facility is documented there. Do not invent one.
3. [AWS CompleteMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html)
   defines body-level errors after initial success headers and conditional completion.
   Full response parsing and exact identity reconciliation remain mandatory.
4. [AWS conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)
   distinguishes unfinished multipart uploads from completed objects. Deduction:
   create-only publication is valuable, but is not an admission/revocation fence.
5. [AWS ListMultipartUploads](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html)
   defines bounded pages and continuation. Deduction: one page/empty observation
   is not proof that a request delayed before its effect cannot later create.
6. [AWS DeleteObjects](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html)
   returns per-key results, including failures. Verify all selected entries; overall
   HTTP success is not batch disposal success.
7. [Botocore Config](https://docs.aws.amazon.com/botocore/latest/reference/config.html)
   documents `total_max_attempts=1`. This is the selected mutation-dispatch profile;
   the actual repository-pinned SDK and every intermediate retry layer need tests.
8. [PostgreSQL explicit locking](https://www.postgresql.org/docs/current/explicit-locking.html)
   covers consistent acquisition order and lock lifetime. Neither row nor session
   advisory locks can revoke effects already sent to a different system.
9. [SQLite transaction control](https://www.sqlite.org/lang_transaction.html)
   documents one writer, BEGIN IMMEDIATE and busy outcomes. Use Riverhog's existing
   writer hook and short database transitions, not network work under a writer.
10. [AWS idempotent API analysis](https://aws.amazon.com/builders-library/making-retries-safe-with-idempotent-APIs/)
   explains ambiguous replies and duplicate effects. This packet uses it as supporting
   background, not as evidence that S3 has that stronger API.

The closure/custody split, exact terminal ledger, conservative unknown policy,
claim-renewal interlock and bounded cleanup algorithm are **design deductions**.
The model tests those deductions under explicitly supplied provider-closure evidence;
it does not establish that an external provider can supply missing evidence.
