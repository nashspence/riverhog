# riverhog_client.ApiClient.add_collection_tag

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-add-collection-tag:c0807bcbe9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-da69f01859"></a>
- <a id="s-ca7232990a"></a>`distribution`: `riverhog-client`
- <a id="s-6d57fc3c61"></a>`module`: `riverhog_client`
- <a id="s-cff4b1744d"></a>`name`: `add_collection_tag`
- <a id="s-3299e9bdba"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-f6b2042650"></a>`unit`: `member`

### Declared structure

- <a id="s-ee0ff1d8b4"></a>`kind`: `"method"`
- <a id="s-4eb2a32e13"></a>`signature`: `"\"(self, collection_id: 'CollectionId', *, tag: 'CollectionTag', operation_id: 'str', expected_revision: 'int', expected_tag_set_identity: 'str') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [a-riverhog-cli collection tag add](../../a-riverhog-cli/cli/a-riverhog-cli-collection-tag-add.md)
- [POST /v1/collections/{collection_id}/tags:add](../../riverhog/http-operations/post-v1-collections-collection-id-tags-add.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-7aba970afe"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.add\_collection\_tag](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L2305)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.add_collection_tag`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 370efb9b04330e979e886c6175e3503df9c694ae9346a13e2272056459220bf1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', *, tag: 'CollectionTag', operation_id: 'str', expected_revision: 'int', expected_tag_set_identity: 'str') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "add_collection_tag",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
