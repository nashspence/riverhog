# stove0_recipe_config.ObserverUse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-observeruse:44485d60df -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f17bdc0fd7"></a>
- <a id="s-8c90f5a752"></a>`distribution`: `stove0-recipe-config`
- <a id="s-d93a0accff"></a>`module`: `stove0_recipe_config`
- <a id="s-33a8caa210"></a>`name`: `ObserverUse`
- <a id="s-68d3868bc0"></a>`unit`: `export`

### Declared structure

- <a id="s-88afe3dfef"></a>`kind`: `"class"`
- <a id="s-be3204f515"></a>`signature`: `"\"(*, registration_id: str, contract_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], artifact_rules: tuple[stove0_recipe_config.models.ArtifactRule, ...] = (ArtifactRule(glob='*', role='stove0.source/v1', media_type=None),), options: dict[str, JsonValue] = <factory>, timeout_seconds: Annotated[int, Ge(ge=1), Le(le=86400)] = 300, maximum_result_bytes: Annotated[int, Ge(ge=1), Le(le=67108864)] = 1048576, retrieval_policy: Literal['available-only', 'allow'] = 'available-only') -> None\""`

#### Validated model schema

<a id="s-17dcc8eea2"></a>
- <a id="s-ecfe5531c7"></a>`title`: ObserverUse
- <a id="s-f9feab4556"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3bb0ec182a"></a>`artifact_rules` | no | type="array"; items=(#/$defs/ArtifactRule) |  |
| <a id="s-5ea6924980"></a>`contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-8699d54c02"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-53a5f97506"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864 |  |
| <a id="s-e4baccd503"></a>`options` | no | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-097102eefb"></a>`registration_id` | yes | type="string" |  |
| <a id="s-163df06ff5"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"] |  |
| <a id="s-4413d8f6e7"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400 |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-33e3f47c25"></a>`ArtifactRule` | type="object"; fields=`glob`, `media_type`, `role`; additional keys=`additionalProperties` |
| <a id="s-43040b55c2"></a>`JsonValue` | empty object |

## Governing policies

- <a id="pa-467f4c26bb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources.md#src-9e1422d2d6) — `reference/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py`

### Machine authority

- `/external_contract/python/stove0_recipe_config.ObserverUse`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: aca05147e2b8db3b2c3b7bd6af54d7374b9e941e6b09920ed92366c36535411d -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ArtifactRule": {
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
        "JsonValue": {}
      },
      "additionalProperties": false,
      "properties": {
        "artifact_rules": {
          "default": [
            {
              "glob": "*",
              "media_type": null,
              "role": "stove0.source/v1"
            }
          ],
          "items": {
            "$ref": "#/$defs/ArtifactRule"
          },
          "title": "Artifact Rules",
          "type": "array"
        },
        "contract_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Contract Id",
          "type": "string"
        },
        "contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Contract Sha256",
          "type": "string"
        },
        "maximum_result_bytes": {
          "default": 1048576,
          "maximum": 67108864,
          "minimum": 1,
          "title": "Maximum Result Bytes",
          "type": "integer"
        },
        "options": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Options",
          "type": "object"
        },
        "registration_id": {
          "title": "Registration Id",
          "type": "string"
        },
        "retrieval_policy": {
          "default": "available-only",
          "enum": [
            "available-only",
            "allow"
          ],
          "title": "Retrieval Policy",
          "type": "string"
        },
        "timeout_seconds": {
          "default": 300,
          "maximum": 86400,
          "minimum": 1,
          "title": "Timeout Seconds",
          "type": "integer"
        }
      },
      "required": [
        "registration_id",
        "contract_id",
        "contract_sha256"
      ],
      "title": "ObserverUse",
      "type": "object"
    },
    "signature": "\"(*, registration_id: str, contract_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], artifact_rules: tuple[stove0_recipe_config.models.ArtifactRule, ...] = (ArtifactRule(glob='*', role='stove0.source/v1', media_type=None),), options: dict[str, JsonValue] = <factory>, timeout_seconds: Annotated[int, Ge(ge=1), Le(le=86400)] = 300, maximum_result_bytes: Annotated[int, Ge(ge=1), Le(le=67108864)] = 1048576, retrieval_policy: Literal['available-only', 'allow'] = 'available-only') -> None\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "ObserverUse",
  "unit": "export"
}
```
