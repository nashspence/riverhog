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
- <a id="s-61a3febd43"></a>`distribution`: `stove0-recipe-config`
- <a id="s-296f220fa8"></a>`module`: `stove0_recipe_config`
- <a id="s-a322ceb539"></a>`name`: `RecipeCoordinationRoute`
- <a id="s-ad1f144e9b"></a>`unit`: `export`

### Declared structure

- <a id="s-832e989e41"></a>`kind`: `"class"`
- <a id="s-cecc52a68f"></a>`signature`: `"\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], when: tuple[stove0_recipe_config.models.FactPredicate, ...] = (), artifact_rules: tuple[stove0_recipe_config.models.ArtifactRule, ...] = (ArtifactRule(glob='*', role='stove0.source/v1', media_type=None),), primary_role: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)]] = None, associated_roles: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...] = (), intent: dict[str, JsonValue] = <factory>, projections: tuple[stove0_recipe_config.models.OperationProjection, ...] = (), kind: Literal['coordination'] = 'coordination', recipe: stove0_protocol.models.RecipeRef) -> None\""`

#### Validated model schema

<a id="s-e926c97cb4"></a>
- <a id="s-0597b4898f"></a>`title`: RecipeCoordinationRoute
- <a id="s-ea5e9d6bc9"></a>`description`: One exact subrecipe selected as a branch-bound coordinator.
- <a id="s-97dd5346cf"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-093628d5d2"></a>`artifact_rules` | no | type="array"; items=(#/$defs/ArtifactRule) |  |
| <a id="s-5ef9c77161"></a>`associated_roles` | no | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-d682624207"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-90ea07b293"></a>`intent` | no | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-d4c669daab"></a>`kind` | no | type="string"; const="coordination" |  |
| <a id="s-d2aa5db056"></a>`primary_role` | no | anyOf=type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" \| type="null" |  |
| <a id="s-943a06dac0"></a>`projections` | no | type="array"; items=(#/$defs/OperationProjection) |  |
| <a id="s-c4f794431e"></a>`recipe` | yes | #/$defs/RecipeRef |  |
| <a id="s-37bc1d07c3"></a>`when` | no | type="array"; items=(#/$defs/FactPredicate) |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-2d17589189"></a>`ArtifactFactBinding` | type="object"; fields=`artifact_id_pointer`, `records_pointer`; additional keys=`additionalProperties`, `required` |
| <a id="s-c69b9baaae"></a>`ArtifactRule` | type="object"; fields=`glob`, `media_type`, `role`; additional keys=`additionalProperties` |
| <a id="s-1f1ff682fb"></a>`FactPredicate` | type="object"; fields=`artifact_facts`, `artifact_roles`, `observation_contract_id`, `operator`, `pointer`, `value`; additional keys=`additionalProperties`, `required` |
| <a id="s-055ffe5fa8"></a>`JsonValue` | empty object |
| <a id="s-b359636fab"></a>`OperationProjection` | type="object"; fields=`destination`, `destination_pointer`, `source`, `source_pointer`; additional keys=`additionalProperties`, `required` |
| <a id="s-d76f5030f1"></a>`RecipeRef` | type="object"; fields=`id`, `revision`, `sha256`; additional keys=`additionalProperties`, `required` |

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

<!-- exact-contract-value: 6b10930c1d8c293735c5572ade3f87af0b24d5a58944368dbeb617a1a6512026 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ArtifactFactBinding": {
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
        "FactPredicate": {
          "additionalProperties": false,
          "properties": {
            "artifact_facts": {
              "anyOf": [
                {
                  "$ref": "#/$defs/ArtifactFactBinding"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "artifact_roles": {
              "default": [],
              "items": {
                "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
                "type": "string"
              },
              "title": "Artifact Roles",
              "type": "array"
            },
            "observation_contract_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "title": "Observation Contract Id",
              "type": "string"
            },
            "operator": {
              "default": "equals",
              "enum": [
                "equals",
                "not-equals",
                "contains",
                "exists"
              ],
              "title": "Operator",
              "type": "string"
            },
            "pointer": {
              "pattern": "^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$",
              "title": "Pointer",
              "type": "string"
            },
            "value": {
              "$ref": "#/$defs/JsonValue",
              "default": null
            }
          },
          "required": [
            "observation_contract_id",
            "pointer"
          ],
          "title": "FactPredicate",
          "type": "object"
        },
        "JsonValue": {},
        "OperationProjection": {
          "additionalProperties": false,
          "description": "One declarative JSON-pointer copy into an operation request.",
          "properties": {
            "destination": {
              "enum": [
                "intent",
                "target-options"
              ],
              "title": "Destination",
              "type": "string"
            },
            "destination_pointer": {
              "pattern": "^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$",
              "title": "Destination Pointer",
              "type": "string"
            },
            "source": {
              "enum": [
                "work-effective-intent",
                "work-evaluation"
              ],
              "title": "Source",
              "type": "string"
            },
            "source_pointer": {
              "pattern": "^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$",
              "title": "Source Pointer",
              "type": "string"
            }
          },
          "required": [
            "source",
            "source_pointer",
            "destination",
            "destination_pointer"
          ],
          "title": "OperationProjection",
          "type": "object"
        },
        "RecipeRef": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "title": "Id",
              "type": "string"
            },
            "revision": {
              "minimum": 1,
              "title": "Revision",
              "type": "integer"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Sha256",
              "type": "string"
            }
          },
          "required": [
            "id",
            "revision",
            "sha256"
          ],
          "title": "RecipeRef",
          "type": "object"
        }
      },
      "additionalProperties": false,
      "description": "One exact subrecipe selected as a branch-bound coordinator.",
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
        "associated_roles": {
          "default": [],
          "items": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "type": "string"
          },
          "title": "Associated Roles",
          "type": "array"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "intent": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Intent",
          "type": "object"
        },
        "kind": {
          "const": "coordination",
          "default": "coordination",
          "title": "Kind",
          "type": "string"
        },
        "primary_role": {
          "anyOf": [
            {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Primary Role"
        },
        "projections": {
          "default": [],
          "items": {
            "$ref": "#/$defs/OperationProjection"
          },
          "title": "Projections",
          "type": "array"
        },
        "recipe": {
          "$ref": "#/$defs/RecipeRef"
        },
        "when": {
          "default": [],
          "items": {
            "$ref": "#/$defs/FactPredicate"
          },
          "title": "When",
          "type": "array"
        }
      },
      "required": [
        "id",
        "recipe"
      ],
      "title": "RecipeCoordinationRoute",
      "type": "object"
    },
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], when: tuple[stove0_recipe_config.models.FactPredicate, ...] = (), artifact_rules: tuple[stove0_recipe_config.models.ArtifactRule, ...] = (ArtifactRule(glob='*', role='stove0.source/v1', media_type=None),), primary_role: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)]] = None, associated_roles: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...] = (), intent: dict[str, JsonValue] = <factory>, projections: tuple[stove0_recipe_config.models.OperationProjection, ...] = (), kind: Literal['coordination'] = 'coordination', recipe: stove0_protocol.models.RecipeRef) -> None\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "RecipeCoordinationRoute",
  "unit": "export"
}
```
