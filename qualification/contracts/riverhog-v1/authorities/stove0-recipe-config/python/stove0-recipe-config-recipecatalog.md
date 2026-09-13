# stove0_recipe_config.RecipeCatalog

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-recipecatalog:094db441fc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-adb0115e53"></a>
| Field | Shape |
|---|---|
| <a id="s-944d65392d"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-4c0c7b5ccc"></a>`distribution` | "stove0-recipe-config" |
| <a id="s-37ab8166e1"></a>`module` | "stove0_recipe_config" |
| <a id="s-6966dfe106"></a>`name` | "RecipeCatalog" |
| <a id="s-548641cb4f"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_recipe_config.RecipeCatalog.load](stove0-recipe-config-recipecatalog-load.md)
- [stove0_recipe_config.RecipeCatalog.operation](stove0-recipe-config-recipecatalog-operation.md)
- [stove0_recipe_config.RecipeCatalog.recipe](stove0-recipe-config-recipecatalog-recipe.md)
- [stove0_recipe_config.RecipeCatalog.sha256](stove0-recipe-config-recipecatalog-sha256.md)
- [stove0_recipe_config.RecipeCatalog.valid_catalog](stove0-recipe-config-recipecatalog-valid-catalog.md)
- [stove0_recipe_config.RecipeCatalog.validation_document](stove0-recipe-config-recipecatalog-validation-document.md)

## Governing policies

- <a id="pa-f209886be8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources.md#src-9e1422d2d6) — `reference/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py`

### Machine authority

- `/external_contract/python/stove0_recipe_config.RecipeCatalog`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d5621f836b7300b13496d9d403f464cbacac1bdec190ca6e901c39e2a5cff578 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "d917d4c47bebfc8abcea36832e60bdf80ca2ac7cd717d0f30305b3d377409fd5",
    "signature": "\"(*, format: Literal['stove0-recipes/v1'] = 'stove0-recipes/v1', operations: tuple[stove0_target_protocol.protocol.OperationContract, ...], recipes: tuple[stove0_recipe_config.models.RecipeDefinition, ...]) -> None\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "RecipeCatalog",
  "unit": "export"
}
```
