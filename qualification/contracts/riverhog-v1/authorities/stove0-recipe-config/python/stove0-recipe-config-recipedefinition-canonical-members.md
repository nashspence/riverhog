# stove0_recipe_config.RecipeDefinition.canonical_members

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-recipedefinition-can-e36f2d680a:4cfff39e62 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d25474d72c"></a>
- <a id="s-d74de4dc9a"></a>`distribution`: `stove0-recipe-config`
- <a id="s-316259bb4c"></a>`module`: `stove0_recipe_config`
- <a id="s-4fd4531ab1"></a>`name`: `canonical_members`
- <a id="s-f083ce5755"></a>`owner`: `stove0_recipe_config.RecipeDefinition`
- <a id="s-2c5478dd58"></a>`unit`: `member`

### Declared structure

- <a id="s-12f4cb48f1"></a>`kind`: `"method"`
- <a id="s-67661a91c1"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [RecipeDefinition](stove0-recipe-config-recipedefinition.md)

## Governing policies

- <a id="pa-8c022e57fd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources.md#src-9e1422d2d6) — `reference/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py`

### Machine authority

- `/external_contract/python/stove0_recipe_config.RecipeDefinition.canonical_members`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bf2ca4b234b533096f8d01058a9707cf610af6678dd3a8725eba9231abba2235 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "canonical_members",
  "owner": "stove0_recipe_config.RecipeDefinition",
  "unit": "member"
}
```
