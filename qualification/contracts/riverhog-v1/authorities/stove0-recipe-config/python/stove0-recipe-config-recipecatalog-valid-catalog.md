# stove0_recipe_config.RecipeCatalog.valid_catalog

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-recipecatalog-valid-catalog:5f03ffab44 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2b26a14469"></a>
- <a id="s-1216b2661c"></a>`distribution`: `stove0-recipe-config`
- <a id="s-c978db869b"></a>`module`: `stove0_recipe_config`
- <a id="s-ed1b37fc25"></a>`name`: `valid_catalog`
- <a id="s-b381d617c4"></a>`owner`: `stove0_recipe_config.RecipeCatalog`
- <a id="s-ca0668c3df"></a>`unit`: `member`

### Declared structure

- <a id="s-a44d2b327d"></a>`kind`: `"method"`
- <a id="s-2aca63982b"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [stove0_recipe_config.RecipeCatalog](stove0-recipe-config-recipecatalog.md)

## Governing policies

- <a id="pa-927691b781"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources.md#src-9e1422d2d6) — `reference/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py`

### Machine authority

- `/external_contract/python/stove0_recipe_config.RecipeCatalog.valid_catalog`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9f9e797f283047068d4ff35df7a504a5e8c0bb418b759d0a6307fc96d2f855a6 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "valid_catalog",
  "owner": "stove0_recipe_config.RecipeCatalog",
  "unit": "member"
}
```
