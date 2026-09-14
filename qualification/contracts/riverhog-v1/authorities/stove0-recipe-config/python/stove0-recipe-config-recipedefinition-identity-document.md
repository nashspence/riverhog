# stove0_recipe_config.RecipeDefinition.identity_document

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-recipedefinition-ide-072860e582:be9ca61635 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0fd24fb32f"></a>
- <a id="s-d023cf2c7d"></a>`distribution`: `stove0-recipe-config`
- <a id="s-6855001a68"></a>`module`: `stove0_recipe_config`
- <a id="s-92a7c77fb8"></a>`name`: `identity_document`
- <a id="s-ecb97a4ddb"></a>`owner`: `stove0_recipe_config.RecipeDefinition`
- <a id="s-f2ffc01ab4"></a>`unit`: `member`

### Declared structure

- <a id="s-4d910fd136"></a>`kind`: `"method"`
- <a id="s-dfc665c357"></a>`signature`: `"\"(self) -> 'dict[str, JsonValue]'\""`

## Maintained corroboration

### Related interface records

- [stove0_recipe_config.RecipeDefinition](stove0-recipe-config-recipedefinition.md)

## Governing policies

- <a id="pa-3aa723fe8a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources.md#src-9e1422d2d6) — `reference/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py`

### Machine authority

- `/external_contract/python/stove0_recipe_config.RecipeDefinition.identity_document`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 44a2733857f069f0a47fe47fd9babbb9f1b766eee5e39e9d9c3f466df0e457a9 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, JsonValue]'\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "identity_document",
  "owner": "stove0_recipe_config.RecipeDefinition",
  "unit": "member"
}
```
