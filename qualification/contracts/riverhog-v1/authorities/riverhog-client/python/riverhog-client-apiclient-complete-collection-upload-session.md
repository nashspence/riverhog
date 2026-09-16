# riverhog_client.ApiClient.complete_collection_upload_session

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-complete-collec-cea91d0ee5:978b7b64dc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-050e3fe877"></a>
- <a id="s-82c546cb59"></a>`distribution`: `riverhog-client`
- <a id="s-3b2c9e4970"></a>`module`: `riverhog_client`
- <a id="s-0a55cd7f97"></a>`name`: `complete_collection_upload_session`
- <a id="s-b3a005b88f"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-2a3f844d26"></a>`unit`: `member`

### Declared structure

- <a id="s-e3f20dbc0d"></a>`kind`: `"method"`
- <a id="s-27ffed8603"></a>`signature`: `"\"(self, collection_id: 'CollectionId') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [piggity collection upload start](../../piggity/cli/piggity-collection-upload-start.md)
- [POST /v1/collection-upload-sessions/{collection_id}/complete](../../riverhog/http-operations/post-v1-collection-upload-sessions-collection-id-complete.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-eb31797339"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`
- **Client method:** [packages/riverhog-client/src/riverhog_client/client.py::ApiClient.complete_collection_upload_session](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L1430)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.complete_collection_upload_session`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 85e0afa758f17d58482c2e7705f7e2cfc39338f1d3113a2465c00d1c340e98bf -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "complete_collection_upload_session",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
