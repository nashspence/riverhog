# riverhog_client.ApiClient.create_or_resume_collection_upload_session

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-create-or-resum-f84e68612c:f414754ecb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5d904b10b5"></a>
- <a id="s-f3bc3a7524"></a>`distribution`: `riverhog-client`
- <a id="s-1e0b87df51"></a>`module`: `riverhog_client`
- <a id="s-27afda93b0"></a>`name`: `create_or_resume_collection_upload_session`
- <a id="s-3d4c7ee6dd"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-5005d0beb3"></a>`unit`: `member`

### Declared structure

- <a id="s-e1a4baa9ca"></a>`kind`: `"method"`
- <a id="s-c2d3a9347f"></a>`signature`: `"\"(self, idempotency_key: 'CollectionUploadIdempotencyKey', *, ingest_source: 'str \| None' = None, description: 'CollectionDescription \| None' = None, tags: 'Sequence[CollectionTag]' = (), initial_tag_set_identity: 'str', archive_store: 'ArchiveStoreName \| None' = None, event_context: 'Mapping[str, Any] \| None' = None, provenance_mode: 'ProvenanceMode' = 'captured', provenance_omission_reason: 'str \| None' = None, custody_mode: 'CollectionUploadCustodyMode' = 'producer-retained') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [piggity collection upload start](../../piggity/cli/piggity-collection-upload-start.md)
- [POST /v1/collection-upload-sessions](../../riverhog/http-operations/post-v1-collection-upload-sessions.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-37f824e496"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.create\_or\_resume\_collection\_upload\_session](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L1107)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.create_or_resume_collection_upload_session`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 11b92fec084c37c29489ded27ffcd32b3a7e2d6bf121237a23ca16e66021aea1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, idempotency_key: 'CollectionUploadIdempotencyKey', *, ingest_source: 'str | None' = None, description: 'CollectionDescription | None' = None, tags: 'Sequence[CollectionTag]' = (), initial_tag_set_identity: 'str', archive_store: 'ArchiveStoreName | None' = None, event_context: 'Mapping[str, Any] | None' = None, provenance_mode: 'ProvenanceMode' = 'captured', provenance_omission_reason: 'str | None' = None, custody_mode: 'CollectionUploadCustodyMode' = 'producer-retained') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "create_or_resume_collection_upload_session",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
