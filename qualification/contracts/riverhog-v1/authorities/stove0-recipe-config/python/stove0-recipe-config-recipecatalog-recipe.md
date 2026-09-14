# stove0_recipe_config.RecipeCatalog.recipe

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-recipecatalog-recipe:9d6e5ecb35 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c00dd23594"></a>
- <a id="s-a33c992473"></a>`distribution`: `stove0-recipe-config`
- <a id="s-0e41540a49"></a>`module`: `stove0_recipe_config`
- <a id="s-9c20261310"></a>`name`: `recipe`
- <a id="s-e2b202166f"></a>`owner`: `stove0_recipe_config.RecipeCatalog`
- <a id="s-bb55e416cd"></a>`unit`: `member`

### Declared structure

- <a id="s-a0c67e19fd"></a>`kind`: `"method"`
- <a id="s-51c2dbda46"></a>`signature`: `"\"(self, recipe_id: 'str', revision: 'int \| None' = None) -> 'RecipeDefinition'\""`

## Maintained corroboration

### Related interface records

- [RecipeCatalog](stove0-recipe-config-recipecatalog.md)

## Governing policies

- <a id="pa-c50e0f0671"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources.md#src-9e1422d2d6) — `reference/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py`

### Machine authority

- `/external_contract/python/stove0_recipe_config.RecipeCatalog.recipe`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4be83958262a45f39261825eaedc26df7f8b39b0b3833b4ae56655488a420e13 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, recipe_id: 'str', revision: 'int | None' = None) -> 'RecipeDefinition'\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "recipe",
  "owner": "stove0_recipe_config.RecipeCatalog",
  "unit": "member"
}
```
