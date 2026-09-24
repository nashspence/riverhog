# stove0_recipe_config.RecipeDefinition.ref

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-recipedefinition-ref:630c7270f7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d9fa9f3825"></a>
- <a id="s-8e452b672c"></a>`distribution`: `stove0-recipe-config`
- <a id="s-6ef446d4cc"></a>`module`: `stove0_recipe_config`
- <a id="s-5515eda128"></a>`name`: `ref`
- <a id="s-4873146045"></a>`owner`: `stove0_recipe_config.RecipeDefinition`
- <a id="s-6d734b854c"></a>`unit`: `member`

### Declared structure

- <a id="s-0e7e1b74fe"></a>`kind`: `"property"`
- <a id="s-4b180f17b1"></a>`signature`: `"\"(self) -> 'RecipeIdentityRef'\""`

## Maintained corroboration

### Related interface records

- [RecipeDefinition](stove0-recipe-config-recipedefinition.md)

## Governing policies

- <a id="pa-c289eb8ecd"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources/authorities.md#src-9e1422d2d6) — [some-implementations/stove0/packages/recipe-config/src/stove0\_recipe\_config/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py)

### Machine authority

- `/external_contract/python/stove0_recipe_config.RecipeDefinition.ref`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 77be93fbe5cc923861a2e677b46a808befd8a05feb261bacc6f72569b1f6a9a6 -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'RecipeIdentityRef'\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "ref",
  "owner": "stove0_recipe_config.RecipeDefinition",
  "unit": "member"
}
```

</details>
