# generated:stove0-target: OperationContract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:stove0-target-support:generated-stove0-target-operationcontract:85bdd342ae -->

| Audit field | Value |
|---|---|
| Authority | `stove0-target-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 10 |

## Machine authority

- `/external_contract/protocol_schemas/generated:stove0-target/schemas/OperationContract`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:stove0-target` — `reference/stove0/packages/target-support/src/stove0_target_support/schemas.py::target_schema_bundle`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract summary

- `title`: OperationContract
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `contract_sha256` | yes | string |  |
| `effect_receipt_schema` | no | object (2 fields) |  |
| `id` | yes | string |  |
| `inputs` | yes | array |  |
| `intent_schema` | yes | #/$defs/JsonSchemaDocument |  |
| `intent_semantics` | yes | #/$defs/SemanticValidationProfile |  |
| `outputs` | no | array |  |
| `result_kind` | no | string |  |
| `source_retirement_permitted` | no | boolean |  |

### Definitions

| Definition | Shape |
|---|---|
| `InputArtifactContract` | object |
| `JsonSchemaDocument` | object |
| `JsonValue` | object (0 fields) |
| `OutputArtifactContract` | object |
| `SemanticValidationProfile` | object |

## Complete owned contract

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
