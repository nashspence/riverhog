# stove0_core.RecipeCatalog

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-recipecatalog:212333cc41 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ac1bf7f772"></a>
| Field | Shape |
|---|---|
| <a id="s-453c673117"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-9a95dda91f"></a>`distribution` | "stove0-server" |
| <a id="s-e81443c24c"></a>`module` | "stove0_core" |
| <a id="s-71d5956b6b"></a>`name` | "RecipeCatalog" |
| <a id="s-1b9a890ee0"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_core.RecipeCatalog.load](stove0-core-recipecatalog-load.md)
- [stove0_core.RecipeCatalog.operation](stove0-core-recipecatalog-operation.md)
- [stove0_core.RecipeCatalog.recipe](stove0-core-recipecatalog-recipe.md)
- [stove0_core.RecipeCatalog.sha256](stove0-core-recipecatalog-sha256.md)
- [stove0_core.RecipeCatalog.valid_catalog](stove0-core-recipecatalog-valid-catalog.md)
- [stove0_core.RecipeCatalog.validation_document](stove0-core-recipecatalog-validation-document.md)

## Governing policies

- <a id="pa-d040925c78"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RecipeCatalog`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e9b0b70198d561e0c7bc63c602d27f49a31470fe1896448c4a5fdbd06bdb77a4 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "d917d4c47bebfc8abcea36832e60bdf80ca2ac7cd717d0f30305b3d377409fd5",
    "signature": "\"(*, format: Literal['stove0-recipes/v1'] = 'stove0-recipes/v1', operations: tuple[stove0_target_protocol.protocol.OperationContract, ...], recipes: tuple[stove0_recipe_config.models.RecipeDefinition, ...]) -> None\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "RecipeCatalog",
  "unit": "export"
}
```
