# stove0-recipe-config:configuration:recipe-catalog configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:stove0-recipe-config:stove0-recipe-config-configuration-recipe-60aec4b96f:9fbd1881e8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Configuration Documents](index.md) |
| Contract elements | 1 |
| Extent decisions | 39 |

## External contract

<a id="s-e29fd5e2fc"></a>
- <a id="s-b23a81c067"></a>`title`: RecipeCatalog
- <a id="s-8918a16ca6"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-31eae8ce6a"></a>`format` | no | type="string"; const="stove0-recipes/v1" |  |
| <a id="s-5ef17985fb"></a>`operations` | yes | type="array"; items=(#/$defs/OperationContract) |  |
| <a id="s-ab7cd11b07"></a>`recipes` | yes | type="array"; items=(#/$defs/RecipeDefinition) |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-f8059f2549"></a>`ArtifactAssociation` | type="object"; fields=`associated_roles`, `path_identity`, `primary_role`; additional keys=`additionalProperties`, `required` |
| <a id="s-170e951eb4"></a>`ArtifactFactBinding` | type="object"; fields=`artifact_id_pointer`, `records_pointer`; additional keys=`additionalProperties`, `required` |
| <a id="s-6404b53e68"></a>`ArtifactRule` | type="object"; fields=`glob`, `media_type`, `role`; additional keys=`additionalProperties` |
| <a id="s-0ec4242600"></a>`FactPredicate` | type="object"; fields=`artifact_facts`, `artifact_roles`, `observation_contract_id`, `operator`, `pointer`, `value`; additional keys=`additionalProperties`, `required` |
| <a id="s-c1f484a7f4"></a>`InputArtifactContract` | type="object"; fields=`allowed_dispositions`, `maximum`, `minimum`, `role`; additional keys=`additionalProperties`, `required` |
| <a id="s-388c978681"></a>`JsonSchemaDocument` | type="object"; fields=`dialect`, `format_policy`, `id`, `schema`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-a5f92f9e8f"></a>`JsonValue` | empty object |
| <a id="s-15f0f78bba"></a>`ObserverUse` | type="object"; fields=`artifact_rules`, `contract_id`, `contract_sha256`, `maximum_result_bytes`, `options`, `registration_id`, `retrieval_policy`, `timeout_seconds`; additional keys=`additionalProperties`, `required` |
| <a id="s-c65d8491da"></a>`OperationContract` | type="object"; fields=`contract_sha256`, `effect_receipt_schema`, `id`, `inputs`, `intent_schema`, `intent_semantics`, `outputs`, `result_kind`, `source_retirement_permitted`; additional keys=`additionalProperties`, `required` |
| <a id="s-5422541e29"></a>`OperationProjection` | type="object"; fields=`destination`, `destination_pointer`, `source`, `source_pointer`; additional keys=`additionalProperties`, `required` |
| <a id="s-917836a5fe"></a>`OutputArtifactContract` | type="object"; fields=`derived_from_roles`, `maximum`, `minimum`, `role`; additional keys=`additionalProperties`, `required` |
| <a id="s-1ba6e7737f"></a>`RecipeCoordinationRoute` | type="object"; fields=`artifact_rules`, `associated_roles`, `id`, `intent`, `kind`, `primary_role`, `projections`, `recipe`, `when`; additional keys=`additionalProperties`, `required` |
| <a id="s-791feda911"></a>`RecipeDefinition` | type="object"; fields=`allow_derived_inputs`, `artifact_associations`, `event_input_closure`, `id`, `join`, `observers`, `retirement_grace_seconds`, `revision`, `routes`, `source_retirement_policy`, `unmatched_artifact_disposition`; additional keys=`additionalProperties`, `required` |
| <a id="s-788d8a9692"></a>`RecipeJoin` | type="object"; fields=`id`, `input_retrieval_policy`, `intent`, `members`, `operation_id`, `projections`, `target_options`, `target_registration_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-562cd4bcc5"></a>`RecipeJoinMember` | type="object"; fields=`branch_id`, `output_roles`; additional keys=`additionalProperties`, `required` |
| <a id="s-02918f2c2b"></a>`RecipeRef` | type="object"; fields=`id`, `revision`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-c9d6c9febb"></a>`RecipeRoute` | type="object"; fields=`artifact_rules`, `associated_roles`, `id`, `input_retrieval_policy`, `intent`, `kind`, `operation_id`, `primary_role`, `projections`, `target_options`, `target_registration_id`, `when`; additional keys=`additionalProperties`, `required` |
| <a id="s-8c31943090"></a>`SemanticValidationProfile` | type="object"; fields=`conformance_vectors_sha256`, `id`, `profile_sha256`, `rules`; additional keys=`additionalProperties`, `required` |

### Progression, limits, and lifecycle

#### [extent-rule/configuration-composition/v1](../../../policies/index.md#p-dcd344e8e5)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-recipe-config:configuration:recipe-catalog"}; maximum=null; reason="validated-deployment-composition"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-eaa6d3429a"></a>[definition ArtifactAssociation · field associated_roles](#s-f8059f2549) | `cardinality · items · operational_policy` | shared above |
| <a id="s-e59e634c3e"></a>[definition FactPredicate · field artifact_roles](#s-0ec4242600) | `cardinality · items · operational_policy` | shared above |
| <a id="s-0c224251e1"></a>[definition InputArtifactContract · field allowed_dispositions · array value](#s-c1f484a7f4) | `cardinality · items · operational_policy` | shared above |
| <a id="s-9d5e3a6d66"></a>[definition JsonSchemaDocument · field schema](#s-388c978681) | `cardinality · entries · operational_policy` | shared above |
| <a id="s-9d24cae629"></a>[definition ObserverUse · field artifact_rules](#s-15f0f78bba) | `cardinality · items · operational_policy` | shared above |
| <a id="s-c9abdba6cc"></a>[definition ObserverUse · field options](#s-15f0f78bba) | `cardinality · entries · operational_policy` | shared above |
| <a id="s-7504707457"></a>[definition OperationContract · field inputs](#s-c65d8491da) | `cardinality · items · operational_policy` | shared above |
| <a id="s-13d6cea42d"></a>[definition OperationContract · field outputs](#s-c65d8491da) | `cardinality · items · operational_policy` | shared above |
| <a id="s-60dfafbb69"></a>[definition OutputArtifactContract · field derived_from_roles](#s-917836a5fe) | `cardinality · items · operational_policy` | shared above |
| <a id="s-73a10157d5"></a>[definition RecipeCoordinationRoute · field artifact_rules](#s-1ba6e7737f) | `cardinality · items · operational_policy` | shared above |
| <a id="s-d28760d4f8"></a>[definition RecipeCoordinationRoute · field associated_roles](#s-1ba6e7737f) | `cardinality · items · operational_policy` | shared above |
| <a id="s-2a305fe98b"></a>[definition RecipeCoordinationRoute · field intent](#s-1ba6e7737f) | `cardinality · entries · operational_policy` | shared above |
| <a id="s-7186ce6f00"></a>[definition RecipeCoordinationRoute · field projections](#s-1ba6e7737f) | `cardinality · items · operational_policy` | shared above |
| <a id="s-3be045bd6f"></a>[definition RecipeCoordinationRoute · field when](#s-1ba6e7737f) | `cardinality · items · operational_policy` | shared above |
| <a id="s-10c77901d6"></a>[definition RecipeDefinition · field artifact_associations](#s-791feda911) | `cardinality · items · operational_policy` | shared above |
| <a id="s-a8b913aeb1"></a>[definition RecipeDefinition · field observers](#s-791feda911) | `cardinality · items · operational_policy` | shared above |
| <a id="s-8198a7011b"></a>[definition RecipeDefinition · field routes](#s-791feda911) | `cardinality · items · operational_policy` | shared above |
| <a id="s-c682992aa6"></a>[definition RecipeJoin · field intent](#s-788d8a9692) | `cardinality · entries · operational_policy` | shared above |
| <a id="s-05531ef714"></a>[definition RecipeJoin · field members](#s-788d8a9692) | `cardinality · items · operational_policy` | shared above |
| <a id="s-8e44f54012"></a>[definition RecipeJoin · field projections](#s-788d8a9692) | `cardinality · items · operational_policy` | shared above |
| <a id="s-ff5cb6c5fc"></a>[definition RecipeJoin · field target_options](#s-788d8a9692) | `cardinality · entries · operational_policy` | shared above |
| <a id="s-6a4326eecd"></a>[definition RecipeJoinMember · field output_roles](#s-562cd4bcc5) | `cardinality · items · operational_policy` | shared above |
| <a id="s-c07c17bb3a"></a>[definition RecipeRoute · field artifact_rules](#s-c9d6c9febb) | `cardinality · items · operational_policy` | shared above |
| <a id="s-4dd7c0e382"></a>[definition RecipeRoute · field associated_roles](#s-c9d6c9febb) | `cardinality · items · operational_policy` | shared above |
| <a id="s-11242044dd"></a>[definition RecipeRoute · field intent](#s-c9d6c9febb) | `cardinality · entries · operational_policy` | shared above |
| <a id="s-f81f6f5e02"></a>[definition RecipeRoute · field projections](#s-c9d6c9febb) | `cardinality · items · operational_policy` | shared above |
| <a id="s-d8e1c231d2"></a>[definition RecipeRoute · field target_options](#s-c9d6c9febb) | `cardinality · entries · operational_policy` | shared above |
| <a id="s-532d32437b"></a>[definition RecipeRoute · field when](#s-c9d6c9febb) | `cardinality · items · operational_policy` | shared above |
| <a id="s-5868b02446"></a>[definition SemanticValidationProfile · field rules](#s-8c31943090) | `cardinality · items · operational_policy` | shared above |
| [field operations](#s-5ef17985fb) | `cardinality · items · operational_policy` | shared above |
| [field recipes](#s-ab7cd11b07) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-6017c9433b"></a>[definition JsonSchemaDocument · field sha256](#s-388c978681) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-b49223eddd"></a>[definition ObserverUse · field contract_sha256](#s-15f0f78bba) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-fe439b004e"></a>[definition ObserverUse · field maximum_result_bytes](#s-15f0f78bba) | `value · schema-value · contract_max` | maximum=67108864; minimum=1; reason="schema-maximum" |
| <a id="s-e18a52cac6"></a>[definition ObserverUse · field timeout_seconds](#s-15f0f78bba) | `value · schema-value · contract_max` | maximum=86400; minimum=1; reason="schema-maximum" |
| <a id="s-e8e19384d9"></a>[definition OperationContract · field contract_sha256](#s-c65d8491da) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-ad87abf86c"></a>[definition RecipeRef · field sha256](#s-02918f2c2b) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-78cbcbe687"></a>[definition SemanticValidationProfile · field conformance_vectors_sha256 · string value](#s-8c31943090) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-ad6f2a2d63"></a>[definition SemanticValidationProfile · field profile_sha256](#s-8c31943090) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Governing policies

- <a id="pa-d0c3694e90"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-bdcbfcfe05"></a>[extent-rule/configuration-composition/v1](../../../policies/index.md#p-dcd344e8e5)
- <a id="pa-e9e4492790"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration:stove0-recipe-config:configuration:recipe-catalog](../../../evidence/sources.md#src-28686050a7) — `reference/stove0/packages/recipe-config/src/stove0_recipe_config/models.py::RecipeCatalog`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_documents/stove0-recipe-config:configuration:recipe-catalog`

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
