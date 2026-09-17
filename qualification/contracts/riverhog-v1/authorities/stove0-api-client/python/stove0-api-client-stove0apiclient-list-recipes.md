# stove0_api_client.Stove0ApiClient.list_recipes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-list-recipes:271e843a74 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6203ecfebc"></a>
- <a id="s-7f2236884e"></a>`distribution`: `stove0-api-client`
- <a id="s-7c24676ba6"></a>`module`: `stove0_api_client`
- <a id="s-71eb5702ee"></a>`name`: `list_recipes`
- <a id="s-f97a77496c"></a>`owner`: `stove0_api_client.Stove0ApiClient`
- <a id="s-240066d30c"></a>`unit`: `member`

### Declared structure

- <a id="s-63dfb3b104"></a>`kind`: `"method"`
- <a id="s-45ca5eaa24"></a>`signature`: `"\"(self) -> 'RecipeCatalogView'\""`

## Maintained corroboration

### Related interface records

- [stove0 recipe list](../../stove0-client/cli/stove0-recipe-list.md)
- [GET /v1/recipes](../../stove0/http-operations/get-v1-recipes.md)
- [Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-2eb4c36de0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources.md#src-5d52ac5998) — [reference/stove0/packages/api-client/src/stove0\_api\_client/\_\_init\_\_.py](../../../../../../reference/stove0/packages/api-client/src/stove0_api_client/__init__.py)
- **Client method:** [reference/stove0/packages/api-client/src/stove0\_api\_client/client.py::Stove0ApiClient.list\_recipes](../../../../../../reference/stove0/packages/api-client/src/stove0_api_client/client.py#L146)

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.list_recipes`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e98d804eec16922d22d9298082aa269d65e6f2ca5d40dae6d8aa90b4da4853b0 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'RecipeCatalogView'\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "list_recipes",
  "owner": "stove0_api_client.Stove0ApiClient",
  "unit": "member"
}
```

</details>
