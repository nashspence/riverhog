# stove0_recipe_config.RecipeCatalog.operation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-recipecatalog-operation:e051b16019 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-55f8555e78"></a>
- <a id="s-0f577b0ebc"></a>`distribution`: `stove0-recipe-config`
- <a id="s-1d73239d39"></a>`module`: `stove0_recipe_config`
- <a id="s-17f977b5cf"></a>`name`: `operation`
- <a id="s-aa2469113f"></a>`owner`: `stove0_recipe_config.RecipeCatalog`
- <a id="s-68de33872b"></a>`unit`: `member`

### Declared structure

- <a id="s-f6c5e3ab0d"></a>`kind`: `"method"`
- <a id="s-85d1ea071d"></a>`signature`: `"\"(self, operation_id: 'str') -> 'OperationContract'\""`

## Maintained corroboration

### Related interface records

- [stove0_recipe_config.RecipeCatalog](stove0-recipe-config-recipecatalog.md)

## Governing policies

- <a id="pa-10352f5a1e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources.md#src-9e1422d2d6) — `reference/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py`

### Machine authority

- `/external_contract/python/stove0_recipe_config.RecipeCatalog.operation`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 475d34dcb4717ea340f93ddae0205f43652910ffe58f1cecbcb304872d522521 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, operation_id: 'str') -> 'OperationContract'\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "operation",
  "owner": "stove0_recipe_config.RecipeCatalog",
  "unit": "member"
}
```
