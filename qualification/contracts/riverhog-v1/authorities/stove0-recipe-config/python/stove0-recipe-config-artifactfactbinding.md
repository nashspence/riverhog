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
- <a id="s-c5aaa97f60"></a>`distribution`: `stove0-recipe-config`
- <a id="s-4f742f4d87"></a>`module`: `stove0_recipe_config`
- <a id="s-a179d0463a"></a>`name`: `ArtifactFactBinding`
- <a id="s-27573bb8bf"></a>`unit`: `export`

### Declared structure

- <a id="s-c6ab09111c"></a>`kind`: `"class"`
- <a id="s-fd117ea240"></a>`signature`: `"\"(*, records_pointer: Annotated[str, _PydanticGeneralMetadata(pattern='^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$')], artifact_id_pointer: Annotated[str, _PydanticGeneralMetadata(pattern='^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$')] = '/artifact_id') -> None\""`

#### Validated model schema

<a id="s-6a7298d114"></a>
- <a id="s-9f36602a95"></a>`title`: ArtifactFactBinding
- <a id="s-0cd1e84e3f"></a>`description`: Locate subject-keyed records inside one observer's declared facts schema.
- <a id="s-3ad3a18495"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0b5428e709"></a>`artifact_id_pointer` | no | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |
| <a id="s-9b2325b091"></a>`records_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |

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

<!-- exact-contract-value: 2f42e4f192ac97e1a3e15f9298d67b2a24a0174dcfe125d0383536db57679554 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "description": "Locate subject-keyed records inside one observer's declared facts schema.",
      "properties": {
        "artifact_id_pointer": {
          "default": "/artifact_id",
          "pattern": "^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$",
          "title": "Artifact Id Pointer",
          "type": "string"
        },
        "records_pointer": {
          "pattern": "^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$",
          "title": "Records Pointer",
          "type": "string"
        }
      },
      "required": [
        "records_pointer"
      ],
      "title": "ArtifactFactBinding",
      "type": "object"
    },
    "signature": "\"(*, records_pointer: Annotated[str, _PydanticGeneralMetadata(pattern='^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$')], artifact_id_pointer: Annotated[str, _PydanticGeneralMetadata(pattern='^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$')] = '/artifact_id') -> None\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "ArtifactFactBinding",
  "unit": "export"
}
```
