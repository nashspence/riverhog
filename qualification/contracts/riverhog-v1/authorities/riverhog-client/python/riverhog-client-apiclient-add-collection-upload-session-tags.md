# riverhog_client.ApiClient.add_collection_upload_session_tags

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-add-collection-8c2e152303:91db074c86 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5058a013a9"></a>
- <a id="s-c23230bd98"></a>`distribution`: `riverhog-client`
- <a id="s-5135377c2a"></a>`module`: `riverhog_client`
- <a id="s-ab641f0a1a"></a>`name`: `add_collection_upload_session_tags`
- <a id="s-c1b4bfbe34"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-47d2946da1"></a>`unit`: `member`

### Declared structure

- <a id="s-9a8edb2a3d"></a>`kind`: `"method"`
- <a id="s-88a9001282"></a>`signature`: `"\"(self, collection_id: 'CollectionId', tags: 'Sequence[CollectionTag]') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [a-riverhog-cli collection upload start](../../a-riverhog-cli/cli/a-riverhog-cli-collection-upload-start.md)
- [POST /v1/collection-upload-sessions/{collection_id}/tags](../../riverhog/http-operations/post-v1-collection-upload-sessions-collection-id-tags.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-d717222478"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.add\_collection\_upload\_session\_tags](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L1173)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.add_collection_upload_session_tags`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0ead984199c530eb3b0ec1d646c64279885c6a6ff6395265936cddc6355ddaf6 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', tags: 'Sequence[CollectionTag]') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "add_collection_upload_session_tags",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
