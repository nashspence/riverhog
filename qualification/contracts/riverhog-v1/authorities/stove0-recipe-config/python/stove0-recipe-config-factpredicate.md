# stove0_recipe_config.FactPredicate

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-factpredicate:7c6bd52834 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f3a0437ad8"></a>
| Field | Shape |
|---|---|
| <a id="s-d39a31e979"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-fb3c031c1f"></a>`distribution` | "stove0-recipe-config" |
| <a id="s-008ced19c2"></a>`module` | "stove0_recipe_config" |
| <a id="s-be3f48f4cc"></a>`name` | "FactPredicate" |
| <a id="s-163e3a8f4c"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_recipe_config.FactPredicate.valid_scope](stove0-recipe-config-factpredicate-valid-scope.md)

## Governing policies

- <a id="pa-a7b6668dba"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources.md#src-9e1422d2d6) — `reference/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py`

### Machine authority

- `/external_contract/python/stove0_recipe_config.FactPredicate`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e4d0508a2df34e479c428670975e869d01c476396e93bc1367533b5a313ffd54 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "de5a579067305377d33e95cc6b3c461652ad1ef30db493109919896dab179fa4",
    "signature": "\"(*, observation_contract_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], artifact_roles: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...] = (), artifact_facts: stove0_recipe_config.models.ArtifactFactBinding | None = None, pointer: Annotated[str, _PydanticGeneralMetadata(pattern='^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$')], operator: Literal['equals', 'not-equals', 'contains', 'exists'] = 'equals', value: JsonValue = None) -> None\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "FactPredicate",
  "unit": "export"
}
```
