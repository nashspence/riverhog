# riverhog_client.ApiClient.remove_collection_tag

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-remove-collection-tag:1e5b208dc3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d90e12800c"></a>
- <a id="s-780ea8589b"></a>`distribution`: `riverhog-client`
- <a id="s-60002458d4"></a>`module`: `riverhog_client`
- <a id="s-17cfb0e1c1"></a>`name`: `remove_collection_tag`
- <a id="s-ebb3544097"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-2e8087d11f"></a>`unit`: `member`

### Declared structure

- <a id="s-9cebf5d87d"></a>`kind`: `"method"`
- <a id="s-6098c99230"></a>`signature`: `"\"(self, collection_id: 'CollectionId', *, tag: 'CollectionTag', operation_id: 'str', expected_revision: 'int', expected_tag_set_identity: 'str') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [a-riverhog-cli collection tag remove](../../a-riverhog-cli/cli/a-riverhog-cli-collection-tag-remove.md)
- [POST /v1/collections/{collection_id}/tags:remove](../../riverhog/http-operations/post-v1-collections-collection-id-tags-remove.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-446b996495"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.remove\_collection\_tag](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L2324)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.remove_collection_tag`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f243eb1f81511ed49fcb30fa1ef92211b5925830d468b4aadff13bcf1dded161 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', *, tag: 'CollectionTag', operation_id: 'str', expected_revision: 'int', expected_tag_set_identity: 'str') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "remove_collection_tag",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
