# riverhog_client.ApiClient.discard_collection_upload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-discard-collection-upload:46e5b5828a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-88ad954d3f"></a>
- <a id="s-89a562dc67"></a>`distribution`: `riverhog-client`
- <a id="s-ff8bc16b7e"></a>`module`: `riverhog_client`
- <a id="s-e833e5741c"></a>`name`: `discard_collection_upload`
- <a id="s-9f9dbc7542"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-bf384d9d64"></a>`unit`: `member`

### Declared structure

- <a id="s-ee066b72f9"></a>`kind`: `"method"`
- <a id="s-77a1ae66a0"></a>`signature`: `"\"(self, collection_id: 'CollectionId', *, challenge: 'str') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [a-riverhog-cli collection upload discard](../../a-riverhog-cli/cli/a-riverhog-cli-collection-upload-discard.md)
- [POST /v1/collection-upload-sessions/{collection_id}/discard](../../riverhog/http-operations/post-v1-collection-upload-sessions-collection-id-discard.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-839dc9aeab"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.discard\_collection\_upload](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L1480)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.discard_collection_upload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: af96759823a5bfd833a10f528d80b67ea667c78d505691034477221b6920d054 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', *, challenge: 'str') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "discard_collection_upload",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
