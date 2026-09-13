# stove0_recipe_config.RecipeJoin

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-recipejoin:659c99f914 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ae20f3e87c"></a>
| Field | Shape |
|---|---|
| <a id="s-f7aed4864b"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-3b1279601b"></a>`distribution` | "stove0-recipe-config" |
| <a id="s-b562b69b0c"></a>`module` | "stove0_recipe_config" |
| <a id="s-5489320d70"></a>`name` | "RecipeJoin" |
| <a id="s-3f431ca44b"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_recipe_config.RecipeJoin.canonical_projections](stove0-recipe-config-recipejoin-canonical-projections.md)

## Governing policies

- <a id="pa-1ce220b2b3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources.md#src-9e1422d2d6) — `reference/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py`

### Machine authority

- `/external_contract/python/stove0_recipe_config.RecipeJoin`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 53589d45805f6a5d79df5038c4a1f096032ed21c1002dc70d58d04f09d570975 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "5fef83beb5dcd069ffebfd5201605f3b9a08ae25dab3e7e9504bf4b01666daf8",
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], members: Annotated[tuple[stove0_recipe_config.models.RecipeJoinMember, ...], MinLen(min_length=2)], operation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], target_registration_id: str, intent: dict[str, JsonValue] = <factory>, target_options: dict[str, JsonValue] = <factory>, projections: tuple[stove0_recipe_config.models.OperationProjection, ...] = (), input_retrieval_policy: Literal['available-only', 'allow'] = 'available-only') -> None\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "RecipeJoin",
  "unit": "export"
}
```
