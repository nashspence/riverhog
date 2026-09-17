# riverhog_client.ApiClient.list_collection_upload_session_files

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-list-collection-5bf87239b0:2688e408f9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8cfb3d707b"></a>
- <a id="s-9c9801c7e3"></a>`distribution`: `riverhog-client`
- <a id="s-d03a0a5f7d"></a>`module`: `riverhog_client`
- <a id="s-c9df6a8e9a"></a>`name`: `list_collection_upload_session_files`
- <a id="s-67ff55361f"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-c47e9843ac"></a>`unit`: `member`

### Declared structure

- <a id="s-8b59044c17"></a>`kind`: `"method"`
- <a id="s-9d966efacd"></a>`signature`: `"\"(self, collection_id: 'CollectionId', *, page_size: 'int' = 25, page_token: 'str \| None' = None) -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [piggity collection upload files](../../piggity/cli/piggity-collection-upload-files.md)
- [GET /v1/collection-upload-sessions/{collection_id}/files](../../riverhog/http-operations/get-v1-collection-upload-sessions-collection-id-files.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-1974a6905d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.list\_collection\_upload\_session\_files](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L1215)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.list_collection_upload_session_files`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 927821cdcd4b03623002713bf085b0c6cf37d656c372e89d03057f49612e2114 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', *, page_size: 'int' = 25, page_token: 'str | None' = None) -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "list_collection_upload_session_files",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
