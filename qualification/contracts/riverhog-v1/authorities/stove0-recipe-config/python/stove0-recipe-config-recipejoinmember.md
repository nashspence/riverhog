# stove0_recipe_config.RecipeJoinMember

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-recipejoinmember:f4505e5f41 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cf9be6eb91"></a>
| Field | Shape |
|---|---|
| <a id="s-676a986a86"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-04369fd660"></a>`distribution` | "stove0-recipe-config" |
| <a id="s-cb5b8af0b6"></a>`module` | "stove0_recipe_config" |
| <a id="s-b43aa245d4"></a>`name` | "RecipeJoinMember" |
| <a id="s-d2ffdc69e6"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_recipe_config.RecipeJoinMember.canonical_roles](stove0-recipe-config-recipejoinmember-canonical-roles.md)

## Governing policies

- <a id="pa-91ba8af705"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources.md#src-9e1422d2d6) — `reference/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py`

### Machine authority

- `/external_contract/python/stove0_recipe_config.RecipeJoinMember`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b3c9ac5263de0255d5fe8f9923acca755a0ab1572d798584149d2f2d69fe8db7 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "f96f0cc79aff67369a99fa4bb8d4b8f29e89bdd258e990af58d3e937574ad43f",
    "signature": "\"(*, branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], output_roles: Annotated[tuple[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], MinLen(min_length=1)]) -> None\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "RecipeJoinMember",
  "unit": "export"
}
```
