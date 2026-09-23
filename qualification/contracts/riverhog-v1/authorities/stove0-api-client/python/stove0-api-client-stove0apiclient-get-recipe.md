# stove0_api_client.Stove0ApiClient.get_recipe

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-get-recipe:847ee18261 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-43d4cd4d55"></a>
- <a id="s-f40434153d"></a>`distribution`: `stove0-api-client`
- <a id="s-1e47186095"></a>`module`: `stove0_api_client`
- <a id="s-f3b2cbabb6"></a>`name`: `get_recipe`
- <a id="s-a817db1fd0"></a>`owner`: `stove0_api_client.Stove0ApiClient`
- <a id="s-149d46dfc9"></a>`unit`: `member`

### Declared structure

- <a id="s-0a7ffe03ad"></a>`kind`: `"method"`
- <a id="s-aa3bca1e06"></a>`signature`: `"\"(self, recipe_id: 'str', *, revision: 'int \| None' = None) -> 'RecipeView'\""`

## Maintained corroboration

### Related interface records

- [stove0 recipe show](../../a-stove0-cli/cli/stove0-recipe-show.md)
- [GET /v1/recipes/{recipe_id}](../../stove0/http-operations/get-v1-recipes-recipe-id.md)
- [Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-ab8809db3a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources/authorities.md#src-5d52ac5998) — [some-implementations/stove0/packages/api-client/src/stove0\_api\_client/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/api-client/src/stove0_api_client/__init__.py)
- **Client method:** [some-implementations/stove0/packages/api-client/src/stove0\_api\_client/client.py::Stove0ApiClient.get\_recipe](../../../../../../some-implementations/stove0/packages/api-client/src/stove0_api_client/client.py#L149)

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.get_recipe`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 884b28e8d5f24672c46ec6b1513c9fbe8e2cc655b30938cc0b178e09c2692370 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, recipe_id: 'str', *, revision: 'int | None' = None) -> 'RecipeView'\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "get_recipe",
  "owner": "stove0_api_client.Stove0ApiClient",
  "unit": "member"
}
```

</details>
