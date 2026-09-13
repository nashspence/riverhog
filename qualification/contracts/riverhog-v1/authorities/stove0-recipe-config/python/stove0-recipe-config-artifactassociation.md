# stove0_recipe_config.ArtifactAssociation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-artifactassociation:c4e9f6274a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4e6310bc6a"></a>
| Field | Shape |
|---|---|
| <a id="s-b819a9b925"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-6206c2ac9e"></a>`distribution` | "stove0-recipe-config" |
| <a id="s-894408085a"></a>`module` | "stove0_recipe_config" |
| <a id="s-710a1efd07"></a>`name` | "ArtifactAssociation" |
| <a id="s-9e54fc0abb"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_recipe_config.ArtifactAssociation.canonical_roles](stove0-recipe-config-artifactassociation-canonical-roles.md)

## Governing policies

- <a id="pa-6838c8af2b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources.md#src-9e1422d2d6) — `reference/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py`

### Machine authority

- `/external_contract/python/stove0_recipe_config.ArtifactAssociation`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 354f73d431424cdd31f99f67404350b02540158718ea5acf9146d5aba8800644 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "00a5b3ff7e9d421eedaa499625122b72ab475c426ae33f3193ca4bb26500ed7d",
    "signature": "\"(*, primary_role: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], associated_roles: Annotated[tuple[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], MinLen(min_length=1)], path_identity: Literal['same-parent-stem'] = 'same-parent-stem') -> None\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "ArtifactAssociation",
  "unit": "export"
}
```
