# stove0_recipe_config.RecipeDefinition.ref

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-recipedefinition-ref:630c7270f7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d9fa9f3825"></a>
| Field | Shape |
|---|---|
| <a id="s-31024eaa88"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-8e452b672c"></a>`distribution` | "stove0-recipe-config" |
| <a id="s-6ef446d4cc"></a>`module` | "stove0_recipe_config" |
| <a id="s-5515eda128"></a>`name` | "ref" |
| <a id="s-4873146045"></a>`owner` | "stove0_recipe_config.RecipeDefinition" |
| <a id="s-6d734b854c"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_recipe_config.RecipeDefinition](stove0-recipe-config-recipedefinition.md)

## Governing policies

- <a id="pa-c289eb8ecd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources.md#src-9e1422d2d6) — `reference/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py`

### Machine authority

- `/external_contract/python/stove0_recipe_config.RecipeDefinition.ref`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 35d965cbb87bf051b9febbf338025434a3a681f3ba673426cca858b77e472b8a -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'RecipeRef'\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "ref",
  "owner": "stove0_recipe_config.RecipeDefinition",
  "unit": "member"
}
```
