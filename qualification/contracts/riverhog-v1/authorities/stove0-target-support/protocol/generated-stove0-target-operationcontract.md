# generated:stove0-target: OperationContract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:stove0-target-support:generated-stove0-target-operationcontract:85bdd342ae -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](index.md#f-3862b77c4f) |
| Contract elements | 1 |
| Extent decisions | 10 |

## External contract

<a id="s-eb1390484d"></a>
- <a id="s-640878f7dc"></a>`title`: OperationContract
- <a id="s-cc7fe3474d"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d662de9b15"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-772086af5f"></a>`effect_receipt_schema` | no | anyOf=#/$defs/JsonSchemaDocument \| type="null" |  |
| <a id="s-702fc5907b"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-f5e91f55d7"></a>`inputs` | yes | type="array"; minItems=1; items=(#/$defs/InputArtifactContract) |  |
| <a id="s-51f7145810"></a>`intent_schema` | yes | #/$defs/JsonSchemaDocument |  |
| <a id="s-7b1634eba7"></a>`intent_semantics` | yes | #/$defs/SemanticValidationProfile |  |
| <a id="s-b55e379a61"></a>`outputs` | no | type="array"; items=(#/$defs/OutputArtifactContract) |  |
| <a id="s-fc20921336"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"] |  |
| <a id="s-09f0f21d9f"></a>`source_retirement_permitted` | no | type="boolean" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-5f66949083"></a>`InputArtifactContract` | type="object"; fields=`allowed_dispositions`, `maximum`, `minimum`, `role`; additional keys=`additionalProperties`, `required` |
| <a id="s-a7a5e8c1b9"></a>`JsonSchemaDocument` | type="object"; fields=`dialect`, `format_policy`, `id`, `schema`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-003a178d94"></a>`JsonValue` | empty object |
| <a id="s-63772a2004"></a>`OutputArtifactContract` | type="object"; fields=`derived_from_roles`, `maximum`, `minimum`, `role`; additional keys=`additionalProperties`, `required` |
| <a id="s-c4cc0dce83"></a>`SemanticValidationProfile` | type="object"; fields=`conformance_vectors_sha256`, `id`, `profile_sha256`, `rules`; additional keys=`additionalProperties`, `required` |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-target-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field inputs](#s-f5e91f55d7) | `cardinality · items · operational_policy` | shared above |
| [field outputs](#s-b55e379a61) | `cardinality · items · operational_policy` | shared above |
| <a id="s-52aee30098"></a>[definition InputArtifactContract · field allowed_dispositions · array value](#s-5f66949083) | `cardinality · items · operational_policy` | shared above |
| <a id="s-4683f85a56"></a>[definition JsonSchemaDocument · field schema](#s-a7a5e8c1b9) | `cardinality · entries · operational_policy` | shared above |
| <a id="s-db0de128fe"></a>[definition OutputArtifactContract · field derived_from_roles](#s-63772a2004) | `cardinality · items · operational_policy` | shared above |
| <a id="s-2f2dbe7f7f"></a>[definition SemanticValidationProfile · field rules](#s-c4cc0dce83) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field contract_sha256](#s-d662de9b15) | `length · characters · fixed` | shared above |
| <a id="s-3d54e4a533"></a>[definition JsonSchemaDocument · field sha256](#s-a7a5e8c1b9) | `length · characters · fixed` | shared above |
| <a id="s-4052e4cd9d"></a>[definition SemanticValidationProfile · field conformance_vectors_sha256 · string value](#s-c4cc0dce83) | `length · characters · fixed` | shared above |
| <a id="s-133a5bb76e"></a>[definition SemanticValidationProfile · field profile_sha256](#s-c4cc0dce83) | `length · characters · fixed` | shared above |

## Governing policies

- <a id="pa-f5be151035"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-a1a648cec1"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-49017f31d2"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:stove0-target](../../../evidence/sources.md#src-2c42f9d39a) — `reference/stove0/packages/target-support/src/stove0_target_support/schemas.py::target_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-target/schemas/OperationContract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: aa2a5d676e27f1328d9ac0508bb514c28355ff594b7e72c052731d4fbc4edd22 -->

```json
{
  "$defs": {
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
}
```
