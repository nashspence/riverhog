# riverhog_client.ApiClient.heartbeat_collection_upload_session

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-heartbeat-colle-1ce87f799f:65f53c8d22 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-687f223f65"></a>
- <a id="s-72a0103951"></a>`distribution`: `riverhog-client`
- <a id="s-148bf6a1e4"></a>`module`: `riverhog_client`
- <a id="s-01c3a33a79"></a>`name`: `heartbeat_collection_upload_session`
- <a id="s-85524fe25f"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-2a27dcdf7b"></a>`unit`: `member`

### Declared structure

- <a id="s-9436601600"></a>`kind`: `"method"`
- <a id="s-f764cfd892"></a>`signature`: `"\"(self, collection_id: 'CollectionId') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [POST /v1/collection-upload-sessions/{collection_id}/heartbeat](../../riverhog/http-operations/post-v1-collection-upload-sessions-collection-id-heartbeat.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-be0b8919df"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.heartbeat\_collection\_upload\_session](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L1466)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.heartbeat_collection_upload_session`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a21c749e628e8313418dc136c73ff2fe0f58c5fa8df54d49586485f4cdc73391 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "heartbeat_collection_upload_session",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
