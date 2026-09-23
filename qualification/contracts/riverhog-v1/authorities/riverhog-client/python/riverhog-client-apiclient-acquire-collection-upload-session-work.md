# riverhog_client.ApiClient.acquire_collection_upload_session_work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-acquire-collect-b0e94f75b2:b993409cae -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ee21cbea4b"></a>
- <a id="s-2a549796ab"></a>`distribution`: `riverhog-client`
- <a id="s-ba499159fc"></a>`module`: `riverhog_client`
- <a id="s-61047c8560"></a>`name`: `acquire_collection_upload_session_work`
- <a id="s-2d7bab63a7"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-7eb84a7944"></a>`unit`: `member`

### Declared structure

- <a id="s-b20643b503"></a>`kind`: `"method"`
- <a id="s-659fde9054"></a>`signature`: `"\"(self, collection_id: 'CollectionId', *, limit: 'int' = 16) -> 'CollectionUploadWorkBatchDocument'\""`

## Maintained corroboration

### Related interface records

- [a-riverhog-cli collection upload start](../../a-riverhog-cli/cli/a-riverhog-cli-collection-upload-start.md)
- [GET /v1/collection-upload-sessions/{collection_id}/work](../../riverhog/http-operations/get-v1-collection-upload-sessions-collection-id-work.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-4e673fc76e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.acquire\_collection\_upload\_session\_work](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L1494)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.acquire_collection_upload_session_work`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6c9083d8a8d229e7df0d73903f8be4ee9410c9b5381f177f50763e15320d9d41 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', *, limit: 'int' = 16) -> 'CollectionUploadWorkBatchDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "acquire_collection_upload_session_work",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
