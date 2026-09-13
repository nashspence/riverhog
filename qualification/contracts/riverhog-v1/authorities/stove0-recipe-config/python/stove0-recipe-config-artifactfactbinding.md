# stove0_recipe_config.ArtifactFactBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-artifactfactbinding:3a8064f78f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-41df3e659e"></a>
| Field | Shape |
|---|---|
| <a id="s-fc63b6e845"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-c5aaa97f60"></a>`distribution` | "stove0-recipe-config" |
| <a id="s-4f742f4d87"></a>`module` | "stove0_recipe_config" |
| <a id="s-a179d0463a"></a>`name` | "ArtifactFactBinding" |
| <a id="s-27573bb8bf"></a>`unit` | "export" |

## Governing policies

- <a id="pa-4df5bd8d76"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources.md#src-9e1422d2d6) — `reference/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py`

### Machine authority

- `/external_contract/python/stove0_recipe_config.ArtifactFactBinding`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f2d5b485461628b193062b844189ff9d6079e4625d71734f97668dcc7e1a23af -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "be2e042f0f939418ce250f41c4b0fb8be5f7c53668a50eff8f53679cc92e6567",
    "signature": "\"(*, records_pointer: Annotated[str, _PydanticGeneralMetadata(pattern='^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$')], artifact_id_pointer: Annotated[str, _PydanticGeneralMetadata(pattern='^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$')] = '/artifact_id') -> None\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "ArtifactFactBinding",
  "unit": "export"
}
```
