# stove0_recipe_config.RecipeCoordinationRoute

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-recipecoordinationroute:d8200ed7b0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8c926e290e"></a>
| Field | Shape |
|---|---|
| <a id="s-fe622c226f"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-61a3febd43"></a>`distribution` | "stove0-recipe-config" |
| <a id="s-296f220fa8"></a>`module` | "stove0_recipe_config" |
| <a id="s-a322ceb539"></a>`name` | "RecipeCoordinationRoute" |
| <a id="s-ad1f144e9b"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_recipe_config.RecipeCoordinationRoute.coordination_projections_target_intent_only](stove0-recipe-config-recipecoordinationroute-coordination-projections-target-intent-only.md)

## Governing policies

- <a id="pa-44e40bb11d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources.md#src-9e1422d2d6) — `reference/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py`

### Machine authority

- `/external_contract/python/stove0_recipe_config.RecipeCoordinationRoute`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a029b67aaef7b70ee4e2b6fd63852e0a7d1f49a57a679a8eccb180c0c60729e3 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "7c52772dd554a9f5ee79ef94c53b55f649cf8904cadd74bc0cebcf7f950ec1b4",
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], when: tuple[stove0_recipe_config.models.FactPredicate, ...] = (), artifact_rules: tuple[stove0_recipe_config.models.ArtifactRule, ...] = (ArtifactRule(glob='*', role='stove0.source/v1', media_type=None),), primary_role: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)]] = None, associated_roles: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...] = (), intent: dict[str, JsonValue] = <factory>, projections: tuple[stove0_recipe_config.models.OperationProjection, ...] = (), kind: Literal['coordination'] = 'coordination', recipe: stove0_protocol.models.RecipeRef) -> None\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "RecipeCoordinationRoute",
  "unit": "export"
}
```
