# stove0_core.RecipeCatalog.recipe

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-recipecatalog-recipe:75e1219ef7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4e800795c8"></a>
- <a id="s-91f4bc90d3"></a>`distribution`: `stove0-server`
- <a id="s-64f25182eb"></a>`module`: `stove0_core`
- <a id="s-06572e1819"></a>`name`: `recipe`
- <a id="s-65b062e0c1"></a>`owner`: `stove0_core.RecipeCatalog`
- <a id="s-7f053c77a2"></a>`unit`: `member`

### Declared structure

- <a id="s-e319a04f5a"></a>`kind`: `"method"`
- <a id="s-05f4e51ef7"></a>`signature`: `"\"(self, recipe_id: 'str', revision: 'int \| None' = None) -> 'RecipeDefinition'\""`

## Maintained corroboration

### Related interface records

- [RecipeCatalog](stove0-core-recipecatalog.md)

## Governing policies

- <a id="pa-5207a8294e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RecipeCatalog.recipe`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 91bdc6dcaa316cadbe804918d0f557aadef12157a8fd905821fc26fa2260fe57 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, recipe_id: 'str', revision: 'int | None' = None) -> 'RecipeDefinition'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "recipe",
  "owner": "stove0_core.RecipeCatalog",
  "unit": "member"
}
```

</details>
