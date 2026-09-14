# stove0_core.RecipeCatalog

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-recipecatalog:212333cc41 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ac1bf7f772"></a>
- <a id="s-9a95dda91f"></a>`distribution`: `stove0-server`
- <a id="s-e81443c24c"></a>`module`: `stove0_core`
- <a id="s-71d5956b6b"></a>`name`: `RecipeCatalog`
- <a id="s-1b9a890ee0"></a>`unit`: `export`

### Declared structure

- <a id="s-dd34918488"></a>`kind`: `"class"`
- <a id="s-f47e6b3ead"></a>`signature`: `"\"(*, format: Literal['stove0-recipes/v1'] = 'stove0-recipes/v1', operations: tuple[stove0_target_protocol.protocol.OperationContract, ...], recipes: tuple[stove0_recipe_config.models.RecipeDefinition, ...]) -> None\""`

#### Validated model schema

<a id="s-7eab2c5d80"></a>
- <a id="s-60e4dd322d"></a>`title`: RecipeCatalog
- <a id="s-b6f14aa630"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4c73cd4611"></a>`format` | no | type="string"; const="stove0-recipes/v1" |  |
| <a id="s-e8c4db3c48"></a>`operations` | yes | type="array"; items=(#/$defs/OperationContract) |  |
| <a id="s-d2dd1a7761"></a>`recipes` | yes | type="array"; items=(#/$defs/RecipeDefinition) |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-4ea94d6958"></a>`ArtifactAssociation` | type="object"; fields=`associated_roles`, `path_identity`, `primary_role`; additional keys=`additionalProperties`, `required` |
| <a id="s-f35ed8e977"></a>`ArtifactFactBinding` | type="object"; fields=`artifact_id_pointer`, `records_pointer`; additional keys=`additionalProperties`, `required` |
| <a id="s-321bcd0181"></a>`ArtifactRule` | type="object"; fields=`glob`, `media_type`, `role`; additional keys=`additionalProperties` |
| <a id="s-faa5d4d6cc"></a>`FactPredicate` | type="object"; fields=`artifact_facts`, `artifact_roles`, `observation_contract_id`, `operator`, `pointer`, `value`; additional keys=`additionalProperties`, `required` |
| <a id="s-893e7a1885"></a>`InputArtifactContract` | type="object"; fields=`allowed_dispositions`, `maximum`, `minimum`, `role`; additional keys=`additionalProperties`, `required` |
| <a id="s-0b52936ac5"></a>`JsonSchemaDocument` | type="object"; fields=`dialect`, `format_policy`, `id`, `schema`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-655fdbe95a"></a>`JsonValue` | empty object |
| <a id="s-ead92c6441"></a>`ObserverUse` | type="object"; fields=`artifact_rules`, `contract_id`, `contract_sha256`, `maximum_result_bytes`, `options`, `registration_id`, `retrieval_policy`, `timeout_seconds`; additional keys=`additionalProperties`, `required` |
| <a id="s-42d3f33c91"></a>`OperationContract` | type="object"; fields=`contract_sha256`, `effect_receipt_schema`, `id`, `inputs`, `intent_schema`, `intent_semantics`, `outputs`, `result_kind`, `source_retirement_permitted`; additional keys=`additionalProperties`, `required` |
| <a id="s-bd29d8e8f0"></a>`OperationProjection` | type="object"; fields=`destination`, `destination_pointer`, `source`, `source_pointer`; additional keys=`additionalProperties`, `required` |
| <a id="s-4ebe2c544b"></a>`OutputArtifactContract` | type="object"; fields=`derived_from_roles`, `maximum`, `minimum`, `role`; additional keys=`additionalProperties`, `required` |
| <a id="s-0e7e1d3689"></a>`RecipeCoordinationRoute` | type="object"; fields=`artifact_rules`, `associated_roles`, `id`, `intent`, `kind`, `primary_role`, `projections`, `recipe`, `when`; additional keys=`additionalProperties`, `required` |
| <a id="s-d7a456910c"></a>`RecipeDefinition` | type="object"; fields=`allow_derived_inputs`, `artifact_associations`, `event_input_closure`, `id`, `join`, `observers`, `retirement_grace_seconds`, `revision`, `routes`, `source_retirement_policy`, `unmatched_artifact_disposition`; additional keys=`additionalProperties`, `required` |
| <a id="s-ffedb1726b"></a>`RecipeJoin` | type="object"; fields=`id`, `input_retrieval_policy`, `intent`, `members`, `operation_id`, `projections`, `target_options`, `target_registration_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-33c0aa00c7"></a>`RecipeJoinMember` | type="object"; fields=`branch_id`, `output_roles`; additional keys=`additionalProperties`, `required` |
| <a id="s-968cd63522"></a>`RecipeRef` | type="object"; fields=`id`, `revision`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-e0a2848845"></a>`RecipeRoute` | type="object"; fields=`artifact_rules`, `associated_roles`, `id`, `input_retrieval_policy`, `intent`, `kind`, `operation_id`, `primary_role`, `projections`, `target_options`, `target_registration_id`, `when`; additional keys=`additionalProperties`, `required` |
| <a id="s-467a54d056"></a>`SemanticValidationProfile` | type="object"; fields=`conformance_vectors_sha256`, `id`, `profile_sha256`, `rules`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [stove0_core.RecipeCatalog.load](stove0-core-recipecatalog-load.md)
- [stove0_core.RecipeCatalog.operation](stove0-core-recipecatalog-operation.md)
- [stove0_core.RecipeCatalog.recipe](stove0-core-recipecatalog-recipe.md)
- [stove0_core.RecipeCatalog.sha256](stove0-core-recipecatalog-sha256.md)
- [stove0_core.RecipeCatalog.valid_catalog](stove0-core-recipecatalog-valid-catalog.md)
- [stove0_core.RecipeCatalog.validation_document](stove0-core-recipecatalog-validation-document.md)

## Governing policies

- <a id="pa-d040925c78"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RecipeCatalog`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 981fa3f708eb8fea5664f794f0cdd9ae6bf459078cbdb5136e2731fdcec97b47 -->

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
        "InputArtifactContract": {
          "additionalProperties": false,
          "properties": {
            "allowed_dispositions": {
              "anyOf": [
                {
                  "items": {
                    "enum": [
                      "transformed",
                      "preserved",
                      "omitted",
                      "rejected"
                    ],
                    "type": "string"
                  },
                  "type": "array"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Allowed Dispositions"
            },
            "maximum": {
              "anyOf": [
                {
                  "minimum": 1,
                  "type": "integer"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Maximum"
            },
            "minimum": {
              "default": 1,
              "minimum": 0,
              "title": "Minimum",
              "type": "integer"
            },
            "role": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "title": "Role",
              "type": "string"
            }
          },
          "required": [
            "role"
          ],
          "title": "InputArtifactContract",
          "type": "object"
        },
        "JsonSchemaDocument": {
          "additionalProperties": false,
          "properties": {
            "dialect": {
              "const": "https://json-schema.org/draft/2020-12/schema",
              "default": "https://json-schema.org/draft/2020-12/schema",
              "title": "Dialect",
              "type": "string"
            },
            "format_policy": {
              "const": "annotation-only",
              "default": "annotation-only",
              "title": "Format Policy",
              "type": "string"
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "title": "Id",
              "type": "string"
            },
            "schema": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "title": "Schema",
              "type": "object"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Sha256",
              "type": "string"
            }
          },
          "required": [
            "id",
            "sha256",
            "schema"
          ],
          "title": "JsonSchemaDocument",
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
        "OperationContract": {
          "additionalProperties": false,
          "properties": {
            "contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Contract Sha256",
              "type": "string"
            },
            "effect_receipt_schema": {
              "anyOf": [
                {
                  "$ref": "#/$defs/JsonSchemaDocument"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "title": "Id",
              "type": "string"
            },
            "inputs": {
              "items": {
                "$ref": "#/$defs/InputArtifactContract"
              },
              "minItems": 1,
              "title": "Inputs",
              "type": "array"
            },
            "intent_schema": {
              "$ref": "#/$defs/JsonSchemaDocument"
            },
            "intent_semantics": {
              "$ref": "#/$defs/SemanticValidationProfile"
            },
            "outputs": {
              "default": [],
              "items": {
                "$ref": "#/$defs/OutputArtifactContract"
              },
              "title": "Outputs",
              "type": "array"
            },
            "result_kind": {
              "default": "collection",
              "enum": [
                "collection",
                "external-effect"
              ],
              "title": "Result Kind",
              "type": "string"
            },
            "source_retirement_permitted": {
              "default": false,
              "title": "Source Retirement Permitted",
              "type": "boolean"
            }
          },
          "required": [
            "id",
            "intent_schema",
            "intent_semantics",
            "inputs",
            "contract_sha256"
          ],
          "title": "OperationContract",
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
        "OutputArtifactContract": {
          "additionalProperties": false,
          "properties": {
            "derived_from_roles": {
              "items": {
                "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
                "type": "string"
              },
              "minItems": 1,
              "title": "Derived From Roles",
              "type": "array"
            },
            "maximum": {
              "anyOf": [
                {
                  "minimum": 1,
                  "type": "integer"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Maximum"
            },
            "minimum": {
              "default": 1,
              "minimum": 0,
              "title": "Minimum",
              "type": "integer"
            },
            "role": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "title": "Role",
              "type": "string"
            }
          },
          "required": [
            "role",
            "derived_from_roles"
          ],
          "title": "OutputArtifactContract",
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
        "RecipeDefinition": {
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
        },
        "SemanticValidationProfile": {
          "additionalProperties": false,
          "properties": {
            "conformance_vectors_sha256": {
              "anyOf": [
                {
                  "pattern": "^[0-9a-f]{64}$",
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Conformance Vectors Sha256"
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "title": "Id",
              "type": "string"
            },
            "profile_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Profile Sha256",
              "type": "string"
            },
            "rules": {
              "items": {
                "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
                "type": "string"
              },
              "minItems": 1,
              "title": "Rules",
              "type": "array"
            }
          },
          "required": [
            "id",
            "rules",
            "profile_sha256"
          ],
          "title": "SemanticValidationProfile",
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "format": {
          "const": "stove0-recipes/v1",
          "default": "stove0-recipes/v1",
          "title": "Format",
          "type": "string"
        },
        "operations": {
          "items": {
            "$ref": "#/$defs/OperationContract"
          },
          "title": "Operations",
          "type": "array"
        },
        "recipes": {
          "items": {
            "$ref": "#/$defs/RecipeDefinition"
          },
          "title": "Recipes",
          "type": "array"
        }
      },
      "required": [
        "operations",
        "recipes"
      ],
      "title": "RecipeCatalog",
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-recipes/v1'] = 'stove0-recipes/v1', operations: tuple[stove0_target_protocol.protocol.OperationContract, ...], recipes: tuple[stove0_recipe_config.models.RecipeDefinition, ...]) -> None\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "RecipeCatalog",
  "unit": "export"
}
```
