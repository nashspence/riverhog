# stove0_recipe_config.ArtifactRule

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-artifactrule:7c77a20c2e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ef82a9c846"></a>
- <a id="s-26c0d4cf80"></a>`distribution`: `stove0-recipe-config`
- <a id="s-3c280dad97"></a>`module`: `stove0_recipe_config`
- <a id="s-8b914e6399"></a>`name`: `ArtifactRule`
- <a id="s-1d0dd49bab"></a>`unit`: `export`

### Declared structure

- <a id="s-21672940ba"></a>`kind`: `"class"`
- <a id="s-01b7952fa3"></a>`signature`: `"\"(*, glob: str = '*', role: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)] = 'stove0.source/v1', media_type: str \| None = None) -> None\""`

#### Validated model schema

<a id="s-2b974c363c"></a>
- <a id="s-8617f0f769"></a>`title`: ArtifactRule
- <a id="s-0097cc3cbf"></a>`description`: Classify one path; first matching rule wins.
- <a id="s-302aa07d6c"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c11fcc0c93"></a>`glob` | no | type="string" |  |
| <a id="s-33fa30455d"></a>`media_type` | no | anyOf=type="string" \| type="null" |  |
| <a id="s-88a9a14717"></a>`role` | no | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

## Governing policies

- <a id="pa-176045925b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources.md#src-9e1422d2d6) — `reference/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py`

### Machine authority

- `/external_contract/python/stove0_recipe_config.ArtifactRule`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c6aa2e2fdc17854b14bc6b8602ccae524aec4a62b502a7b741f72bf350646262 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "description": "Classify one path; first matching rule wins.",
      "properties": {
        "glob": {
          "default": "*",
          "title": "Glob",
          "type": "string"
        },
        "media_type": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Media Type"
        },
        "role": {
          "default": "stove0.source/v1",
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Role",
          "type": "string"
        }
      },
      "title": "ArtifactRule",
      "type": "object"
    },
    "signature": "\"(*, glob: str = '*', role: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)] = 'stove0.source/v1', media_type: str | None = None) -> None\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "ArtifactRule",
  "unit": "export"
}
```
