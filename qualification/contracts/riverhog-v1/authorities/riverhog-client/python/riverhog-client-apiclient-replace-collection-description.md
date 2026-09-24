# riverhog_client.ApiClient.replace_collection_description

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-replace-collect-9aa8fc3ac0:83b9e53422 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-705888d839"></a>
- <a id="s-aa2517e70a"></a>`distribution`: `riverhog-client`
- <a id="s-57a99362ea"></a>`module`: `riverhog_client`
- <a id="s-0e096ffad1"></a>`name`: `replace_collection_description`
- <a id="s-1929d86ebd"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-6d633ec218"></a>`unit`: `member`

### Declared structure

- <a id="s-bf8f08f25b"></a>`kind`: `"method"`
- <a id="s-4b29fce3c7"></a>`signature`: `"\"(self, collection_id: 'CollectionId', description: 'CollectionDescription \| None', *, expected_identity: 'str') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [a-riverhog-cli collection describe](../../a-riverhog-cli/cli/a-riverhog-cli-collection-describe.md)
- [PUT /v1/collections/{collection_id}/description](../../riverhog/http-operations/put-v1-collections-collection-id-description.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-3f4d4c323c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.replace\_collection\_description](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L1601)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.replace_collection_description`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1c20cf6222a347d75cf20e743aea1ffe515836c9ff9f17c5f809e4da961aeb63 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', description: 'CollectionDescription | None', *, expected_identity: 'str') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "replace_collection_description",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
