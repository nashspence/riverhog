# stove0_recipe_config.RecipeDefinition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-recipedefinition:c7c878259b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0119d3013a"></a>
- <a id="s-e4c3252c02"></a>`distribution`: `stove0-recipe-config`
- <a id="s-20906aff87"></a>`module`: `stove0_recipe_config`
- <a id="s-805f9f2ed8"></a>`name`: `RecipeDefinition`
- <a id="s-41370f7180"></a>`unit`: `export`

### Declared structure

- <a id="s-be43a28ae7"></a>`kind`: `"class"`
- <a id="s-90380c0a70"></a>`signature`: `"\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], revision: Annotated[int, Ge(ge=1)], event_input_closure: Literal['single-finalized-collection'] = 'single-finalized-collection', artifact_associations: tuple[stove0_recipe_config.models.ArtifactAssociation, ...] = (), observers: tuple[stove0_recipe_config.models.ObserverUse, ...] = (), routes: Annotated[tuple[Annotated[stove0_recipe_config.models.RecipeRoute \| stove0_recipe_config.models.RecipeCoordinationRoute, FieldInfo(annotation=NoneType, required=True, discriminator='kind')], ...], MinLen(min_length=1)], unmatched_artifact_disposition: Literal['retain-in-source', 'reject-work'], allow_derived_inputs: bool = False, source_retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', retirement_grace_seconds: Annotated[int, Ge(ge=0)] = 0, join: stove0_recipe_config.models.RecipeJoin \| None = None) -> None\""`

#### Validated model schema

<a id="s-28244ebf9c"></a>
- <a id="s-7f2aa7491d"></a>`title`: RecipeDefinition
- <a id="s-96c4c66c30"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-11491989e3"></a>`allow_derived_inputs` | no | type="boolean" |  |
| <a id="s-e35545034f"></a>`artifact_associations` | no | type="array"; items=(#/$defs/ArtifactAssociation) |  |
| <a id="s-130d6caa97"></a>`event_input_closure` | no | type="string"; const="single-finalized-collection" |  |
| <a id="s-080eb4720c"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-006a434cc1"></a>`join` | no | anyOf=#/$defs/RecipeJoin \| type="null" |  |
| <a id="s-fd94ee5952"></a>`observers` | no | type="array"; items=(#/$defs/ObserverUse) |  |
| <a id="s-447069a1b8"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0 |  |
| <a id="s-d6c2d22f1a"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-a6aff0f971"></a>`routes` | yes | type="array"; minItems=1; items=(oneOf=#/$defs/RecipeRoute \| #/$defs/RecipeCoordinationRoute; additional keys=`discriminator`) |  |
| <a id="s-535f78d4ac"></a>`source_retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"] |  |
| <a id="s-2712f1d519"></a>`unmatched_artifact_disposition` | yes | type="string"; enum=["retain-in-source","reject-work"] |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-cbe0c73c25"></a>`ArtifactAssociation` | type="object"; fields=`associated_roles`, `path_identity`, `primary_role`; additional keys=`additionalProperties`, `required` |
| <a id="s-3a0df3e117"></a>`ArtifactFactBinding` | type="object"; fields=`artifact_id_pointer`, `records_pointer`; additional keys=`additionalProperties`, `required` |
| <a id="s-043cb19d8b"></a>`ArtifactRule` | type="object"; fields=`glob`, `media_type`, `role`; additional keys=`additionalProperties` |
| <a id="s-555441cc57"></a>`FactPredicate` | type="object"; fields=`artifact_facts`, `artifact_roles`, `observation_contract_id`, `operator`, `pointer`, `value`; additional keys=`additionalProperties`, `required` |
| <a id="s-547817020e"></a>`JsonValue` | empty object |
| <a id="s-f32a9323a1"></a>`ObserverUse` | type="object"; fields=`artifact_rules`, `contract_id`, `contract_sha256`, `maximum_result_bytes`, `options`, `registration_id`, `retrieval_policy`, `timeout_seconds`; additional keys=`additionalProperties`, `required` |
| <a id="s-d265eb178c"></a>`OperationProjection` | type="object"; fields=`destination`, `destination_pointer`, `source`, `source_pointer`; additional keys=`additionalProperties`, `required` |
| <a id="s-13711c075a"></a>`RecipeCoordinationRoute` | type="object"; fields=`artifact_rules`, `associated_roles`, `id`, `intent`, `kind`, `primary_role`, `projections`, `recipe`, `when`; additional keys=`additionalProperties`, `required` |
| <a id="s-109200a17a"></a>`RecipeJoin` | type="object"; fields=`id`, `input_retrieval_policy`, `intent`, `members`, `operation_id`, `projections`, `target_options`, `target_registration_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-e7fea4754a"></a>`RecipeJoinMember` | type="object"; fields=`branch_id`, `output_roles`; additional keys=`additionalProperties`, `required` |
| <a id="s-fd35bcb24f"></a>`RecipeRef` | type="object"; fields=`id`, `revision`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-748c3d5210"></a>`RecipeRoute` | type="object"; fields=`artifact_rules`, `associated_roles`, `id`, `input_retrieval_policy`, `intent`, `kind`, `operation_id`, `primary_role`, `projections`, `target_options`, `target_registration_id`, `when`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [stove0_recipe_config.RecipeDefinition.canonical_members](stove0-recipe-config-recipedefinition-canonical-members.md)
- [stove0_recipe_config.RecipeDefinition.identity_document](stove0-recipe-config-recipedefinition-identity-document.md)
- [stove0_recipe_config.RecipeDefinition.ref](stove0-recipe-config-recipedefinition-ref.md)
- [stove0_recipe_config.RecipeDefinition.sha256](stove0-recipe-config-recipedefinition-sha256.md)

## Governing policies

- <a id="pa-f994c22696"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources.md#src-9e1422d2d6) — `reference/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py`

### Machine authority

- `/external_contract/python/stove0_recipe_config.RecipeDefinition`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 173f14291fd4242bc0aebc8a847b3ffb4bf4bf1aa5bc02d867d738bd90d11877 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ArtifactAssociation": {
          "additionalProperties": false,
          "description": "Associate classified artifacts without assigning device meaning to Stove0.",
          "properties": {
            "associated_roles": {
              "items": {
                "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
                "type": "string"
              },
              "minItems": 1,
              "title": "Associated Roles",
              "type": "array"
            },
            "path_identity": {
              "const": "same-parent-stem",
              "default": "same-parent-stem",
              "title": "Path Identity",
              "type": "string"
            },
            "primary_role": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "title": "Primary Role",
              "type": "string"
            }
          },
          "required": [
            "primary_role",
            "associated_roles"
          ],
          "title": "ArtifactAssociation",
          "type": "object"
        },
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
        "ObserverUse": {
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
        "RecipeCoordinationRoute": {
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
        "RecipeJoin": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "title": "Id",
              "type": "string"
            },
            "input_retrieval_policy": {
              "default": "available-only",
              "enum": [
                "available-only",
                "allow"
              ],
              "title": "Input Retrieval Policy",
              "type": "string"
            },
            "intent": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "title": "Intent",
              "type": "object"
            },
            "members": {
              "items": {
                "$ref": "#/$defs/RecipeJoinMember"
              },
              "minItems": 2,
              "title": "Members",
              "type": "array"
            },
            "operation_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "title": "Operation Id",
              "type": "string"
            },
            "projections": {
              "default": [],
              "items": {
                "$ref": "#/$defs/OperationProjection"
              },
              "title": "Projections",
              "type": "array"
            },
            "target_options": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "title": "Target Options",
              "type": "object"
            },
            "target_registration_id": {
              "title": "Target Registration Id",
              "type": "string"
            }
          },
          "required": [
            "id",
            "members",
            "operation_id",
            "target_registration_id"
          ],
          "title": "RecipeJoin",
          "type": "object"
        },
        "RecipeJoinMember": {
          "additionalProperties": false,
          "properties": {
            "branch_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "title": "Branch Id",
              "type": "string"
            },
            "output_roles": {
              "items": {
                "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
                "type": "string"
              },
              "minItems": 1,
              "title": "Output Roles",
              "type": "array"
            }
          },
          "required": [
            "branch_id",
            "output_roles"
          ],
          "title": "RecipeJoinMember",
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
        },
        "RecipeRoute": {
          "additionalProperties": false,
          "description": "One ordinary target/effect leaf selected by a recipe.",
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
            "input_retrieval_policy": {
              "default": "available-only",
              "enum": [
                "available-only",
                "allow"
              ],
              "title": "Input Retrieval Policy",
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
              "const": "operation",
              "default": "operation",
              "title": "Kind",
              "type": "string"
            },
            "operation_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "title": "Operation Id",
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
            "target_options": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "title": "Target Options",
              "type": "object"
            },
            "target_registration_id": {
              "title": "Target Registration Id",
              "type": "string"
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
            "operation_id",
            "target_registration_id"
          ],
          "title": "RecipeRoute",
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "allow_derived_inputs": {
          "default": false,
          "title": "Allow Derived Inputs",
          "type": "boolean"
        },
        "artifact_associations": {
          "default": [],
          "items": {
            "$ref": "#/$defs/ArtifactAssociation"
          },
          "title": "Artifact Associations",
          "type": "array"
        },
        "event_input_closure": {
          "const": "single-finalized-collection",
          "default": "single-finalized-collection",
          "title": "Event Input Closure",
          "type": "string"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "join": {
          "anyOf": [
            {
              "$ref": "#/$defs/RecipeJoin"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "observers": {
          "default": [],
          "items": {
            "$ref": "#/$defs/ObserverUse"
          },
          "title": "Observers",
          "type": "array"
        },
        "retirement_grace_seconds": {
          "default": 0,
          "minimum": 0,
          "title": "Retirement Grace Seconds",
          "type": "integer"
        },
        "revision": {
          "minimum": 1,
          "title": "Revision",
          "type": "integer"
        },
        "routes": {
          "items": {
            "discriminator": {
              "mapping": {
                "coordination": "#/$defs/RecipeCoordinationRoute",
                "operation": "#/$defs/RecipeRoute"
              },
              "propertyName": "kind"
            },
            "oneOf": [
              {
                "$ref": "#/$defs/RecipeRoute"
              },
              {
                "$ref": "#/$defs/RecipeCoordinationRoute"
              }
            ]
          },
          "minItems": 1,
          "title": "Routes",
          "type": "array"
        },
        "source_retirement_policy": {
          "default": "retain",
          "enum": [
            "retain",
            "retire-after-verified-output"
          ],
          "title": "Source Retirement Policy",
          "type": "string"
        },
        "unmatched_artifact_disposition": {
          "enum": [
            "retain-in-source",
            "reject-work"
          ],
          "title": "Unmatched Artifact Disposition",
          "type": "string"
        }
      },
      "required": [
        "id",
        "revision",
        "routes",
        "unmatched_artifact_disposition"
      ],
      "title": "RecipeDefinition",
      "type": "object"
    },
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], revision: Annotated[int, Ge(ge=1)], event_input_closure: Literal['single-finalized-collection'] = 'single-finalized-collection', artifact_associations: tuple[stove0_recipe_config.models.ArtifactAssociation, ...] = (), observers: tuple[stove0_recipe_config.models.ObserverUse, ...] = (), routes: Annotated[tuple[Annotated[stove0_recipe_config.models.RecipeRoute | stove0_recipe_config.models.RecipeCoordinationRoute, FieldInfo(annotation=NoneType, required=True, discriminator='kind')], ...], MinLen(min_length=1)], unmatched_artifact_disposition: Literal['retain-in-source', 'reject-work'], allow_derived_inputs: bool = False, source_retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', retirement_grace_seconds: Annotated[int, Ge(ge=0)] = 0, join: stove0_recipe_config.models.RecipeJoin | None = None) -> None\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "RecipeDefinition",
  "unit": "export"
}
```
