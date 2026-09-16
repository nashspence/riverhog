# riverhog_client.ApiClient.get_collection_upload_session_unit

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-get-collection-f9e2a7ccd1:e94ff58dd6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b7c93201da"></a>
- <a id="s-6d4c3cefed"></a>`distribution`: `riverhog-client`
- <a id="s-ca8c22f038"></a>`module`: `riverhog_client`
- <a id="s-3e2ab3c1f5"></a>`name`: `get_collection_upload_session_unit`
- <a id="s-3fef11d0c1"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-7ad5e761a5"></a>`unit`: `member`

### Declared structure

- <a id="s-5f6d1dac87"></a>`kind`: `"method"`
- <a id="s-120814983c"></a>`signature`: `"\"(self, collection_id: 'CollectionId', volume_id: 'CollectionUploadVolumeId', unit: 'CollectionUploadUnitNumber') -> 'CollectionUploadUnitWorkDocument'\""`

## Maintained corroboration

### Related interface records

- [piggity collection upload start](../../piggity/cli/piggity-collection-upload-start.md)
- [GET /v1/collection-upload-sessions/{collection_id}/volumes/{volume_id}/units/{unit}](../../riverhog/http-operations/get-v1-collection-upload-sessions-collection-id-volumes-volume-id-units-unit.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-9c5f94e335"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`
- **Client method:** [packages/riverhog-client/src/riverhog_client/client.py::ApiClient.get_collection_upload_session_unit](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L1499)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.get_collection_upload_session_unit`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 75117c730021b0f0e913dcbb584dede6117bdb198ee940d897fe2a3e96772c6c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', volume_id: 'CollectionUploadVolumeId', unit: 'CollectionUploadUnitNumber') -> 'CollectionUploadUnitWorkDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "get_collection_upload_session_unit",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
