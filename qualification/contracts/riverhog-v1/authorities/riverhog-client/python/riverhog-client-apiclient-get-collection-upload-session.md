# riverhog_client.ApiClient.get_collection_upload_session

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-get-collection-ec7419a74a:d4ab2b5710 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9de774e704"></a>
- <a id="s-1111bf4ecf"></a>`distribution`: `riverhog-client`
- <a id="s-64755ca25b"></a>`module`: `riverhog_client`
- <a id="s-2ea09e1fb3"></a>`name`: `get_collection_upload_session`
- <a id="s-c3509d7399"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-86cea10d61"></a>`unit`: `member`

### Declared structure

- <a id="s-e4c232991e"></a>`kind`: `"method"`
- <a id="s-f0eac71081"></a>`signature`: `"\"(self, collection_id: 'CollectionId') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [piggity collection upload show](../../piggity/cli/piggity-collection-upload-show.md)
- [piggity collection upload start](../../piggity/cli/piggity-collection-upload-start.md)
- [piggity collection upload watch](../../piggity/cli/piggity-collection-upload-watch.md)
- [GET /v1/collection-upload-sessions/{collection_id}](../../riverhog/http-operations/get-v1-collection-upload-sessions-collection-id.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-7c89392428"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`
- **Client method:** [packages/riverhog-client/src/riverhog_client/client.py::ApiClient.get_collection_upload_session](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L1448)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.get_collection_upload_session`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 568f6568561ba81b4fa86c9515da4633e1e4f843e60d05aac2dc9187688ff2ba -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "get_collection_upload_session",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
