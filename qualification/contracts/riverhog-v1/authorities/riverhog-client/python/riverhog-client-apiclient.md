# riverhog_client.ApiClient

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient:fa78a3f3ba -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-10fee65a15"></a>
- <a id="s-cc03e70b26"></a>`distribution`: `riverhog-client`
- <a id="s-8169628702"></a>`module`: `riverhog_client`
- <a id="s-b6046bb143"></a>`name`: `ApiClient`
- <a id="s-a9e9ba8e3f"></a>`unit`: `export`

### Declared structure

- <a id="s-343cc2b7be"></a>`kind`: `"class"`
- <a id="s-a06f2e6ed0"></a>`signature`: `"\"(base_url: 'str \| None' = None, token: 'str \| None' = None, *, allow_insecure_http: 'bool \| None' = None) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [abandon_processing_claim](riverhog-client-apiclient-abandon-processing-claim.md)
- [acknowledge_retrieval_job](riverhog-client-apiclient-acknowledge-retrieval-job.md)
- [acquire_collection_upload_session_work](riverhog-client-apiclient-acquire-collection-upload-session-work.md)
- [add_app_key_access](riverhog-client-apiclient-add-app-key-access.md)
- [add_collection_upload_session_tags](riverhog-client-apiclient-add-collection-upload-session-tags.md)
- [add_collection_tag](riverhog-client-apiclient-add-collection-tag.md)
- [advance_retrieval_plan](riverhog-client-apiclient-advance-retrieval-plan.md)
- [append_collection_upload_session_provenance_journal](riverhog-client-apiclient-append-collection-upload-session-provenance-journal.md)
- [append_processing_claim_inputs](riverhog-client-apiclient-append-processing-claim-inputs.md)
- [append_processing_claim_artifacts](riverhog-client-apiclient-append-processing-claim-artifacts.md)
- [append_processing_capability_artifacts](riverhog-client-apiclient-append-processing-capability-artifacts.md)
- [begin_processing_claim_retirement](riverhog-client-apiclient-begin-processing-claim-retirement.md)
- [cancel_archive_copy_job](riverhog-client-apiclient-cancel-archive-copy-job.md)
- [cancel_collection_upload_session](riverhog-client-apiclient-cancel-collection-upload-session.md)
- [cancel_collection_provenance_verification](riverhog-client-apiclient-cancel-collection-provenance-verification.md)
- [cancel_retrieval_job](riverhog-client-apiclient-cancel-retrieval-job.md)
- [close](riverhog-client-apiclient-close.md)
- [collection_contains_tag](riverhog-client-apiclient-collection-contains-tag.md)
- [collection_provenance_journal_metadata](riverhog-client-apiclient-collection-provenance-journal-metadata.md)
- [complete_collection_upload_session](riverhog-client-apiclient-complete-collection-upload-session.md)
- [create_app_key](riverhog-client-apiclient-create-app-key.md)
- [create_catalog_sync_checkpoint](riverhog-client-apiclient-create-catalog-sync-checkpoint.md)
- [create_collection_upload_session_provenance_journal](riverhog-client-apiclient-create-collection-upload-session-provenance-journal.md)
- [create_or_resume_archive_copy_job](riverhog-client-apiclient-create-or-resume-archive-copy-job.md)
- [create_or_resume_processing_claim](riverhog-client-apiclient-create-or-resume-processing-claim.md)
- [create_or_resume_collection_upload_session](riverhog-client-apiclient-create-or-resume-collection-upload-session.md)
- [create_processing_capability](riverhog-client-apiclient-create-processing-capability.md)
- [create_retrieval_job](riverhog-client-apiclient-create-retrieval-job.md)
- [delete_collection](riverhog-client-apiclient-delete-collection.md)
- [discard_collection_upload](riverhog-client-apiclient-discard-collection-upload.md)
- [download_collection_provenance_journal](riverhog-client-apiclient-download-collection-provenance-journal.md)
- [download_retrieval_file](riverhog-client-apiclient-download-retrieval-file.md)
- [__enter__](riverhog-client-apiclient-enter.md)
- [__exit__](riverhog-client-apiclient-exit.md)
- [get_archive_copy_job](riverhog-client-apiclient-get-archive-copy-job.md)
- [get_archive_store](riverhog-client-apiclient-get-archive-store.md)
- [get_collection_provenance_verification](riverhog-client-apiclient-get-collection-provenance-verification.md)
- [get_collection_upload_session_provenance_journal](riverhog-client-apiclient-get-collection-upload-session-provenance-journal.md)
- [get_collection_file_provenance](riverhog-client-apiclient-get-collection-file-provenance.md)
- [get_collection_derivation](riverhog-client-apiclient-get-collection-derivation.md)
- [get_collection_upload_session](riverhog-client-apiclient-get-collection-upload-session.md)
- [get_collection_upload_session_unit](riverhog-client-apiclient-get-collection-upload-session-unit.md)
- [get_collection](riverhog-client-apiclient-get-collection.md)
- [get_download_quota](riverhog-client-apiclient-get-download-quota.md)
- [get_portable_collection_inventory](riverhog-client-apiclient-get-portable-collection-inventory.md)
- [get_processing_claim_dispositions](riverhog-client-apiclient-get-processing-claim-dispositions.md)
- [get_processing_claim](riverhog-client-apiclient-get-processing-claim.md)
- [get_retrieval_cache_object](riverhog-client-apiclient-get-retrieval-cache-object.md)
- [get_retrieval_job](riverhog-client-apiclient-get-retrieval-job.md)
- [get_retrieval_plan](riverhog-client-apiclient-get-retrieval-plan.md)
- [heartbeat_collection_upload_session](riverhog-client-apiclient-heartbeat-collection-upload-session.md)
- [list_app_key_access](riverhog-client-apiclient-list-app-key-access.md)
- [list_app_keys](riverhog-client-apiclient-list-app-keys.md)
- [list_apps](riverhog-client-apiclient-list-apps.md)
- [list_archive_copy_jobs](riverhog-client-apiclient-list-archive-copy-jobs.md)
- [list_archive_stores](riverhog-client-apiclient-list-archive-stores.md)
- [list_catalog_sync_collections](riverhog-client-apiclient-list-catalog-sync-collections.md)
- [list_catalog_sync_changes](riverhog-client-apiclient-list-catalog-sync-changes.md)
- [list_collection_upload_sessions](riverhog-client-apiclient-list-collection-upload-sessions.md)
- [list_collection_upload_session_files](riverhog-client-apiclient-list-collection-upload-session-files.md)
- [list_collection_provenance_journal_agents](riverhog-client-apiclient-list-collection-provenance-journal-agents.md)
- [list_collection_archive_copies](riverhog-client-apiclient-list-collection-archive-copies.md)
- [list_collection_provenance](riverhog-client-apiclient-list-collection-provenance.md)
- [list_collection_tags](riverhog-client-apiclient-list-collection-tags.md)
- [list_collections](riverhog-client-apiclient-list-collections.md)
- [list_download_quotas](riverhog-client-apiclient-list-download-quotas.md)
- [list_lifecycle_events](riverhog-client-apiclient-list-lifecycle-events.md)
- [list_processing_claim_artifacts](riverhog-client-apiclient-list-processing-claim-artifacts.md)
- [list_processing_claim_inputs](riverhog-client-apiclient-list-processing-claim-inputs.md)
- [list_processing_claim_outcomes](riverhog-client-apiclient-list-processing-claim-outcomes.md)
- [list_processing_claim_disposition_outputs](riverhog-client-apiclient-list-processing-claim-disposition-outputs.md)
- [list_processing_claims](riverhog-client-apiclient-list-processing-claims.md)
- [list_processing_claim_dispositions](riverhog-client-apiclient-list-processing-claim-dispositions.md)
- [list_retrieval_cache_objects](riverhog-client-apiclient-list-retrieval-cache-objects.md)
- [list_retrieval_plan_files](riverhog-client-apiclient-list-retrieval-plan-files.md)
- [list_tags](riverhog-client-apiclient-list-tags.md)
- [plan_archive_copy_retirement](riverhog-client-apiclient-plan-archive-copy-retirement.md)
- [plan_collection_upload_discard](riverhog-client-apiclient-plan-collection-upload-discard.md)
- [plan_collection_deletion](riverhog-client-apiclient-plan-collection-deletion.md)
- [plan_retrieval](riverhog-client-apiclient-plan-retrieval.md)
- [put_collection_upload_session_unit](riverhog-client-apiclient-put-collection-upload-session-unit.md)
- [record_processing_claim_disposition_outputs](riverhog-client-apiclient-record-processing-claim-disposition-outputs.md)
- [record_processing_claim_dispositions](riverhog-client-apiclient-record-processing-claim-dispositions.md)
- [register_collection_upload_session_files](riverhog-client-apiclient-register-collection-upload-session-files.md)
- [register_collection_upload_session_raw_part_digests](riverhog-client-apiclient-register-collection-upload-session-raw-part-digests.md)
- [release_processing_claim](riverhog-client-apiclient-release-processing-claim.md)
- [remove_app_key_access](riverhog-client-apiclient-remove-app-key-access.md)
- [remove_collection_tag](riverhog-client-apiclient-remove-collection-tag.md)
- [renew_processing_claim](riverhog-client-apiclient-renew-processing-claim.md)
- [renew_retrieval_job](riverhog-client-apiclient-renew-retrieval-job.md)
- [replace_app_key_access](riverhog-client-apiclient-replace-app-key-access.md)
- [replace_collection_description](riverhog-client-apiclient-replace-collection-description.md)
- [request_collection_provenance_verification](riverhog-client-apiclient-request-collection-provenance-verification.md)
- [restart_processing_claim](riverhog-client-apiclient-restart-processing-claim.md)
- [retire_archive_copy](riverhog-client-apiclient-retire-archive-copy.md)
- [retrieval_cache_status](riverhog-client-apiclient-retrieval-cache-status.md)
- [revoke_app_key](riverhog-client-apiclient-revoke-app-key.md)
- [rotate_app_key](riverhog-client-apiclient-rotate-app-key.md)
- [seal_collection_upload_session_provenance_journal](riverhog-client-apiclient-seal-collection-upload-session-provenance-journal.md)
- [seal_processing_claim_dispositions](riverhog-client-apiclient-seal-processing-claim-dispositions.md)
- [seal_processing_capability_artifacts](riverhog-client-apiclient-seal-processing-capability-artifacts.md)
- [seal_processing_claim_inputs](riverhog-client-apiclient-seal-processing-claim-inputs.md)
- [seal_processing_claim_artifacts](riverhog-client-apiclient-seal-processing-claim-artifacts.md)
- [seal_processing_claim_plan](riverhog-client-apiclient-seal-processing-claim-plan.md)
- [search](riverhog-client-apiclient-search.md)
- [set_app_key_download_quota](riverhog-client-apiclient-set-app-key-download-quota.md)
- [settle_processing_claim_outcomes](riverhog-client-apiclient-settle-processing-claim-outcomes.md)
- [settle_processing_claim](riverhog-client-apiclient-settle-processing-claim.md)
- [spawn](riverhog-client-apiclient-spawn.md)
- [stream_collection_provenance_journal](riverhog-client-apiclient-stream-collection-provenance-journal.md)
- [stream_retrieval_file](riverhog-client-apiclient-stream-retrieval-file.md)
- [trace_collection_file_provenance](riverhog-client-apiclient-trace-collection-file-provenance.md)
- [upload_collection_upload_session_provenance_journal](riverhog-client-apiclient-upload-collection-upload-session-provenance-journal.md)

## Governing policies

- <a id="pa-a2d8bebc8a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d52f6474b4fdd49b2a5f17c0ca75f6516ec0ade141c0a6a37e9a0447a939ef2b -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(base_url: 'str | None' = None, token: 'str | None' = None, *, allow_insecure_http: 'bool | None' = None) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "ApiClient",
  "unit": "export"
}
```

</details>
