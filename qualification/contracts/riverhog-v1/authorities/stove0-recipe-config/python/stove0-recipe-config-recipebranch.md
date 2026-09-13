# stove0_recipe_config.RecipeBranch

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-recipebranch:f57c4bb834 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fcbac5ad9a"></a>
| Field | Shape |
|---|---|
| <a id="s-439d3533bb"></a>`contract` | type="typing._AnnotatedAlias"; additional keys=`kind` |
| <a id="s-b20ec0cde5"></a>`distribution` | "stove0-recipe-config" |
| <a id="s-4f7667fef0"></a>`module` | "stove0_recipe_config" |
| <a id="s-ac5eb8b960"></a>`name` | "RecipeBranch" |
| <a id="s-734af30bb5"></a>`unit` | "export" |

## Governing policies

- <a id="pa-651cf2f4cd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources.md#src-9e1422d2d6) — `reference/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py`

### Machine authority

- `/external_contract/python/stove0_recipe_config.RecipeBranch`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 28eb25da50a01841e534cd10151503220edea618bd36f09282152de29feb8461 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._AnnotatedAlias"
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "RecipeBranch",
  "unit": "export"
}
```
