# stove0_operator_contracts.RecipeCatalogView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-recipecatalogview:fd87349113 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9b818e8959"></a>
- <a id="s-6d179e3636"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-997203dcf0"></a>`module`: `stove0_operator_contracts`
- <a id="s-b83e24cb66"></a>`name`: `RecipeCatalogView`
- <a id="s-045247b0e7"></a>`unit`: `export`

### Declared structure

- <a id="s-41b7b8ab06"></a>`kind`: `"class"`
- <a id="s-8d9ae17af6"></a>`signature`: `"\"(*, catalog_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], recipes: tuple[stove0_operator_contracts.RecipeView, ...]) -> None\""`

#### Validated model schema

<a id="s-5377ba1876"></a>
- <a id="s-ad59953316"></a>`title`: RecipeCatalogView
- <a id="s-16743d2ca5"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b0fd7d84d2"></a>`catalog_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2972f443c9"></a>`recipes` | yes | type="array"; items=(#/$defs/RecipeView) |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-225da6e177"></a>`ArtifactAssociation` | type="object"; fields=`associated_roles`, `path_identity`, `primary_role`; additional keys=`additionalProperties`, `required` |
| <a id="s-1391b3e97b"></a>`ArtifactFactBinding` | type="object"; fields=`artifact_id_pointer`, `records_pointer`; additional keys=`additionalProperties`, `required` |
| <a id="s-918af1412c"></a>`ArtifactRule` | type="object"; fields=`glob`, `media_type`, `role`; additional keys=`additionalProperties` |
| <a id="s-191cf3054d"></a>`FactPredicate` | type="object"; fields=`artifact_facts`, `artifact_roles`, `observation_contract_id`, `operator`, `pointer`, `value`; additional keys=`additionalProperties`, `required` |
| <a id="s-1193b89a7e"></a>`JsonValue` | empty object |
| <a id="s-24ff768245"></a>`ObserverUse` | type="object"; fields=`artifact_rules`, `contract_id`, `contract_sha256`, `maximum_result_bytes`, `options`, `registration_id`, `retrieval_policy`, `timeout_seconds`; additional keys=`additionalProperties`, `required` |
| <a id="s-65e7d01ae2"></a>`OperationProjection` | type="object"; fields=`destination`, `destination_pointer`, `source`, `source_pointer`; additional keys=`additionalProperties`, `required` |
| <a id="s-8e40bd974f"></a>`RecipeCoordinationRoute` | type="object"; fields=`artifact_rules`, `associated_roles`, `id`, `intent`, `kind`, `primary_role`, `projections`, `recipe`, `when`; additional keys=`additionalProperties`, `required` |
| <a id="s-efb3ec8bd4"></a>`RecipeDefinition` | type="object"; fields=`allow_derived_inputs`, `artifact_associations`, `event_input_closure`, `id`, `join`, `observers`, `retirement_grace_seconds`, `revision`, `routes`, `source_retirement_policy`, `unmatched_artifact_disposition`; additional keys=`additionalProperties`, `required` |
| <a id="s-1fbcbf5a4b"></a>`RecipeJoin` | type="object"; fields=`id`, `input_retrieval_policy`, `intent`, `members`, `operation_id`, `projections`, `target_options`, `target_registration_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-a728e834ee"></a>`RecipeJoinMember` | type="object"; fields=`branch_id`, `output_roles`; additional keys=`additionalProperties`, `required` |
| <a id="s-f2e61bf2dd"></a>`RecipeRef` | type="object"; fields=`id`, `revision`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-26e5942589"></a>`RecipeRoute` | type="object"; fields=`artifact_rules`, `associated_roles`, `id`, `input_retrieval_policy`, `intent`, `kind`, `operation_id`, `primary_role`, `projections`, `target_options`, `target_registration_id`, `when`; additional keys=`additionalProperties`, `required` |
| <a id="s-956c0286d2"></a>`RecipeView` | type="object"; fields=`definition`, `sha256`; additional keys=`additionalProperties`, `required` |

## Governing policies

- <a id="pa-40779a14ee"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.RecipeCatalogView`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 78c8b50ff31ec24c4802056e6aa31183de317a3d65db3ff92e357e30f93590cf -->

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
        "RecipeView": {
          "additionalProperties": false,
          "properties": {
            "definition": {
              "$ref": "#/$defs/RecipeDefinition"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Sha256",
              "type": "string"
            }
          },
          "required": [
            "definition",
            "sha256"
          ],
          "title": "RecipeView",
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "catalog_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Catalog Sha256",
          "type": "string"
        },
        "recipes": {
          "items": {
            "$ref": "#/$defs/RecipeView"
          },
          "title": "Recipes",
          "type": "array"
        }
      },
      "required": [
        "catalog_sha256",
        "recipes"
      ],
      "title": "RecipeCatalogView",
      "type": "object"
    },
    "signature": "\"(*, catalog_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], recipes: tuple[stove0_operator_contracts.RecipeView, ...]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "RecipeCatalogView",
  "unit": "export"
}
```
