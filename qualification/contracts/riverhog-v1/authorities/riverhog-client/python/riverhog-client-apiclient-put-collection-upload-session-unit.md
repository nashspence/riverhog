# riverhog_client.ApiClient.put_collection_upload_session_unit

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-put-collection-fa0fb3eae5:8278adb011 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c3b2548aab"></a>
- <a id="s-539f2721ae"></a>`distribution`: `riverhog-client`
- <a id="s-4a36376610"></a>`module`: `riverhog_client`
- <a id="s-1a1d8dbe5a"></a>`name`: `put_collection_upload_session_unit`
- <a id="s-a94dfc964a"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-79e7cab08e"></a>`unit`: `member`

### Declared structure

- <a id="s-acff0eae30"></a>`kind`: `"method"`
- <a id="s-f75f6f1b6d"></a>`signature`: `"\"(self, collection_id: 'CollectionId', volume_id: 'CollectionUploadVolumeId', unit: 'CollectionUploadUnitNumber', *, plan_sha256: 'str', content: 'bytes') -> 'CollectionUploadUnitWorkDocument'\""`

## Maintained corroboration

### Related interface records

- [piggity collection upload start](../../piggity/cli/piggity-collection-upload-start.md)
- [PUT /v1/collection-upload-sessions/{collection_id}/volumes/{volume_id}/units/{unit}](../../riverhog/http-operations/put-v1-collection-upload-sessions-collection-id-volumes-volume-id-units-unit.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-a473c50b1d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`
- **Client method:** [packages/riverhog-client/src/riverhog_client/client.py::ApiClient.put_collection_upload_session_unit](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L1516)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.put_collection_upload_session_unit`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 768d33e4402f469b05da12474d490885bdea88fbc93599067b4e1d1d791d3eae -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', volume_id: 'CollectionUploadVolumeId', unit: 'CollectionUploadUnitNumber', *, plan_sha256: 'str', content: 'bytes') -> 'CollectionUploadUnitWorkDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "put_collection_upload_session_unit",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
