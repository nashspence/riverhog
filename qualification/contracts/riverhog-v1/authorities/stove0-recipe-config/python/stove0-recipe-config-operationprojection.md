# stove0_recipe_config.OperationProjection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-operationprojection:bb108842e3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-61dd36bc6c"></a>
| Field | Shape |
|---|---|
| <a id="s-3a4ca43760"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-4648f6e419"></a>`distribution` | "stove0-recipe-config" |
| <a id="s-3918a9b4ec"></a>`module` | "stove0_recipe_config" |
| <a id="s-c8dbc8e458"></a>`name` | "OperationProjection" |
| <a id="s-b5754eacc4"></a>`unit` | "export" |

## Governing policies

- <a id="pa-5d0a0cc13c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources.md#src-9e1422d2d6) — `reference/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py`

### Machine authority

- `/external_contract/python/stove0_recipe_config.OperationProjection`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5bdfa082006b1d2f596f96f80519ff0af73bdac95efda353855f0838f77d995f -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "412cc6fe371170daf347f525d51353e79834de6222531002457e205e033c56fa",
    "signature": "\"(*, source: Literal['work-effective-intent', 'work-evaluation'], source_pointer: Annotated[str, _PydanticGeneralMetadata(pattern='^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$')], destination: Literal['intent', 'target-options'], destination_pointer: Annotated[str, _PydanticGeneralMetadata(pattern='^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$')]) -> None\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "OperationProjection",
  "unit": "export"
}
```
