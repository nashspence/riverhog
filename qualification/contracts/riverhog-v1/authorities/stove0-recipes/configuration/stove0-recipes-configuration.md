# stove0-recipes configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:stove0-recipes:stove0-recipes-configuration:d7b3e8d372 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipes](../index.md) |
| Interface | [configuration](index.md) |
| Family | [documents](index.md#f-ea5c21230f) |
| Contract elements | 1 |
| Extent decisions | 39 |

## External contract

<a id="s-221fa0a965"></a>
- <a id="s-8f6f892915"></a>`title`: RecipeCatalog
- <a id="s-768a70141f"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2fd0ccd0e9"></a>`format` | no | type="string"; const="stove0-recipes/v1" |  |
| <a id="s-68008b09db"></a>`operations` | yes | type="array"; items=(#/$defs/OperationContract) |  |
| <a id="s-4171241e9d"></a>`recipes` | yes | type="array"; items=(#/$defs/RecipeDefinition) |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-f8993709b3"></a>`ArtifactAssociation` | type="object"; fields=`associated_roles`, `path_identity`, `primary_role`; additional keys=`additionalProperties`, `required` |
| <a id="s-1ac2e2eec0"></a>`ArtifactFactBinding` | type="object"; fields=`artifact_id_pointer`, `records_pointer`; additional keys=`additionalProperties`, `required` |
| <a id="s-74fb3a8957"></a>`ArtifactRule` | type="object"; fields=`glob`, `media_type`, `role`; additional keys=`additionalProperties` |
| <a id="s-6722cc772a"></a>`FactPredicate` | type="object"; fields=`artifact_facts`, `artifact_roles`, `observation_contract_id`, `operator`, `pointer`, `value`; additional keys=`additionalProperties`, `required` |
| <a id="s-e7c2f1bec3"></a>`InputArtifactContract` | type="object"; fields=`allowed_dispositions`, `maximum`, `minimum`, `role`; additional keys=`additionalProperties`, `required` |
| <a id="s-19363cecd1"></a>`JsonSchemaDocument` | type="object"; fields=`dialect`, `format_policy`, `id`, `schema`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-48ccd9a7b0"></a>`JsonValue` | empty object |
| <a id="s-f0c1398faa"></a>`ObserverUse` | type="object"; fields=`artifact_rules`, `contract_id`, `contract_sha256`, `maximum_result_bytes`, `options`, `registration_id`, `retrieval_policy`, `timeout_seconds`; additional keys=`additionalProperties`, `required` |
| <a id="s-abf24f7610"></a>`OperationContract` | type="object"; fields=`contract_sha256`, `effect_receipt_schema`, `id`, `inputs`, `intent_schema`, `intent_semantics`, `outputs`, `result_kind`, `source_retirement_permitted`; additional keys=`additionalProperties`, `required` |
| <a id="s-90f40eb31e"></a>`OperationProjection` | type="object"; fields=`destination`, `destination_pointer`, `source`, `source_pointer`; additional keys=`additionalProperties`, `required` |
| <a id="s-66f8e50fb5"></a>`OutputArtifactContract` | type="object"; fields=`derived_from_roles`, `maximum`, `minimum`, `role`; additional keys=`additionalProperties`, `required` |
| <a id="s-95a727a469"></a>`RecipeCoordinationRoute` | type="object"; fields=`artifact_rules`, `associated_roles`, `id`, `intent`, `kind`, `primary_role`, `projections`, `recipe`, `when`; additional keys=`additionalProperties`, `required` |
| <a id="s-a74ca70000"></a>`RecipeDefinition` | type="object"; fields=`allow_derived_inputs`, `artifact_associations`, `event_input_closure`, `id`, `join`, `observers`, `retirement_grace_seconds`, `revision`, `routes`, `source_retirement_policy`, `unmatched_artifact_disposition`; additional keys=`additionalProperties`, `required` |
| <a id="s-a82890c5d1"></a>`RecipeJoin` | type="object"; fields=`id`, `input_retrieval_policy`, `intent`, `members`, `operation_id`, `projections`, `target_options`, `target_registration_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-e8351b399f"></a>`RecipeJoinMember` | type="object"; fields=`branch_id`, `output_roles`; additional keys=`additionalProperties`, `required` |
| <a id="s-855a5a5840"></a>`RecipeRef` | type="object"; fields=`id`, `revision`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-6e85f80f5e"></a>`RecipeRoute` | type="object"; fields=`artifact_rules`, `associated_roles`, `id`, `input_retrieval_policy`, `intent`, `kind`, `operation_id`, `primary_role`, `projections`, `target_options`, `target_registration_id`, `when`; additional keys=`additionalProperties`, `required` |
| <a id="s-8f0d0431c9"></a>`SemanticValidationProfile` | type="object"; fields=`conformance_vectors_sha256`, `id`, `profile_sha256`, `rules`; additional keys=`additionalProperties`, `required` |

### Progression, limits, and lifecycle

#### [extent-rule/configuration-composition/v1](../../../policies/index.md#p-dcd344e8e5)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-recipes"}; maximum=null; reason="validated-deployment-composition"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-79e347b06f"></a>[definition ArtifactAssociation · field associated_roles](#s-f8993709b3) | `cardinality · items · operational_policy` | shared above |
| <a id="s-24352bad92"></a>[definition FactPredicate · field artifact_roles](#s-6722cc772a) | `cardinality · items · operational_policy` | shared above |
| <a id="s-6e692f9070"></a>[definition InputArtifactContract · field allowed_dispositions · array value](#s-e7c2f1bec3) | `cardinality · items · operational_policy` | shared above |
| <a id="s-32722a8a21"></a>[definition JsonSchemaDocument · field schema](#s-19363cecd1) | `cardinality · entries · operational_policy` | shared above |
| <a id="s-fbf5087c91"></a>[definition ObserverUse · field artifact_rules](#s-f0c1398faa) | `cardinality · items · operational_policy` | shared above |
| <a id="s-563d8c2f2a"></a>[definition ObserverUse · field options](#s-f0c1398faa) | `cardinality · entries · operational_policy` | shared above |
| <a id="s-759b307522"></a>[definition OperationContract · field inputs](#s-abf24f7610) | `cardinality · items · operational_policy` | shared above |
| <a id="s-a38244c724"></a>[definition OperationContract · field outputs](#s-abf24f7610) | `cardinality · items · operational_policy` | shared above |
| <a id="s-ebd07c4756"></a>[definition OutputArtifactContract · field derived_from_roles](#s-66f8e50fb5) | `cardinality · items · operational_policy` | shared above |
| <a id="s-095d82b988"></a>[definition RecipeCoordinationRoute · field artifact_rules](#s-95a727a469) | `cardinality · items · operational_policy` | shared above |
| <a id="s-5d2c43b0ec"></a>[definition RecipeCoordinationRoute · field associated_roles](#s-95a727a469) | `cardinality · items · operational_policy` | shared above |
| <a id="s-f796207661"></a>[definition RecipeCoordinationRoute · field intent](#s-95a727a469) | `cardinality · entries · operational_policy` | shared above |
| <a id="s-53d552cb6f"></a>[definition RecipeCoordinationRoute · field projections](#s-95a727a469) | `cardinality · items · operational_policy` | shared above |
| <a id="s-e0d4bd8d86"></a>[definition RecipeCoordinationRoute · field when](#s-95a727a469) | `cardinality · items · operational_policy` | shared above |
| <a id="s-b2bd9cd455"></a>[definition RecipeDefinition · field artifact_associations](#s-a74ca70000) | `cardinality · items · operational_policy` | shared above |
| <a id="s-6d97c93546"></a>[definition RecipeDefinition · field observers](#s-a74ca70000) | `cardinality · items · operational_policy` | shared above |
| <a id="s-ee9fba6a6f"></a>[definition RecipeDefinition · field routes](#s-a74ca70000) | `cardinality · items · operational_policy` | shared above |
| <a id="s-e92c72713c"></a>[definition RecipeJoin · field intent](#s-a82890c5d1) | `cardinality · entries · operational_policy` | shared above |
| <a id="s-2f9b4bff1c"></a>[definition RecipeJoin · field members](#s-a82890c5d1) | `cardinality · items · operational_policy` | shared above |
| <a id="s-db7a24389a"></a>[definition RecipeJoin · field projections](#s-a82890c5d1) | `cardinality · items · operational_policy` | shared above |
| <a id="s-c14fc5d54a"></a>[definition RecipeJoin · field target_options](#s-a82890c5d1) | `cardinality · entries · operational_policy` | shared above |
| <a id="s-7fd9a1fd26"></a>[definition RecipeJoinMember · field output_roles](#s-e8351b399f) | `cardinality · items · operational_policy` | shared above |
| <a id="s-05cdf6590e"></a>[definition RecipeRoute · field artifact_rules](#s-6e85f80f5e) | `cardinality · items · operational_policy` | shared above |
| <a id="s-1a01f403d8"></a>[definition RecipeRoute · field associated_roles](#s-6e85f80f5e) | `cardinality · items · operational_policy` | shared above |
| <a id="s-0fe6517726"></a>[definition RecipeRoute · field intent](#s-6e85f80f5e) | `cardinality · entries · operational_policy` | shared above |
| <a id="s-da3367be40"></a>[definition RecipeRoute · field projections](#s-6e85f80f5e) | `cardinality · items · operational_policy` | shared above |
| <a id="s-ea9b1b09f4"></a>[definition RecipeRoute · field target_options](#s-6e85f80f5e) | `cardinality · entries · operational_policy` | shared above |
| <a id="s-18674239aa"></a>[definition RecipeRoute · field when](#s-6e85f80f5e) | `cardinality · items · operational_policy` | shared above |
| <a id="s-3ee14bf8ee"></a>[definition SemanticValidationProfile · field rules](#s-8f0d0431c9) | `cardinality · items · operational_policy` | shared above |
| [field operations](#s-68008b09db) | `cardinality · items · operational_policy` | shared above |
| [field recipes](#s-4171241e9d) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-dee5d7dc51"></a>[definition JsonSchemaDocument · field sha256](#s-19363cecd1) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-81de8e1462"></a>[definition ObserverUse · field contract_sha256](#s-f0c1398faa) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-ac56d82591"></a>[definition ObserverUse · field maximum_result_bytes](#s-f0c1398faa) | `value · schema-value · contract_max` | maximum=67108864; minimum=1; reason="schema-maximum" |
| <a id="s-f63b38f1ec"></a>[definition ObserverUse · field timeout_seconds](#s-f0c1398faa) | `value · schema-value · contract_max` | maximum=86400; minimum=1; reason="schema-maximum" |
| <a id="s-e335a2a2d6"></a>[definition OperationContract · field contract_sha256](#s-abf24f7610) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-36094cbef7"></a>[definition RecipeRef · field sha256](#s-855a5a5840) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-440a691bce"></a>[definition SemanticValidationProfile · field conformance_vectors_sha256 · string value](#s-8f0d0431c9) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-7e99ada17a"></a>[definition SemanticValidationProfile · field profile_sha256](#s-8f0d0431c9) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Governing policies

- <a id="pa-b39452c0b7"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-331c728721"></a>[extent-rule/configuration-composition/v1](../../../policies/index.md#p-dcd344e8e5)
- <a id="pa-9ef170f56d"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration:stove0-recipes](../../../evidence/sources.md#src-49c5fe0ba6) — `reference/stove0/packages/recipe-config/src/stove0_recipe_config/models.py::RecipeCatalog`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_documents/stove0-recipes`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d917d4c47bebfc8abcea36832e60bdf80ca2ac7cd717d0f30305b3d377409fd5 -->

```json
{
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
}
```
