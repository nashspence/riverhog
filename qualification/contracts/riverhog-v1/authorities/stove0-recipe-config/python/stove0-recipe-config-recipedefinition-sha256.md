# stove0_recipe_config.RecipeDefinition.sha256

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-recipedefinition-sha256:459a82f42f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5eff2815f2"></a>
- <a id="s-5595498c1e"></a>`distribution`: `stove0-recipe-config`
- <a id="s-324dec7cc0"></a>`module`: `stove0_recipe_config`
- <a id="s-c0ba2f921b"></a>`name`: `sha256`
- <a id="s-2de7ae0bd9"></a>`owner`: `stove0_recipe_config.RecipeDefinition`
- <a id="s-21bafe70cc"></a>`unit`: `member`

### Declared structure

- <a id="s-b606f751e8"></a>`kind`: `"property"`
- <a id="s-214428b5d9"></a>`signature`: `"\"(self) -> 'str'\""`

## Maintained corroboration

### Related interface records

- [RecipeDefinition](stove0-recipe-config-recipedefinition.md)

## Governing policies

- <a id="pa-f004fe1ace"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources.md#src-9e1422d2d6) — `reference/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py`

### Machine authority

- `/external_contract/python/stove0_recipe_config.RecipeDefinition.sha256`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2786874784408e34720c9ec6e3db6bb5019455ea38b4d0f21c9bdc7bd28d53f3 -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'str'\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "sha256",
  "owner": "stove0_recipe_config.RecipeDefinition",
  "unit": "member"
}
```
