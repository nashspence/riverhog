# generated:stove0-observer: ObserverDescriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:stove0-observer-support:generated-stove0-observer-observerdescriptor:dda498f06c -->

| Audit field | Value |
|---|---|
| Authority | `stove0-observer-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 5 |

## Machine authority

- `/external_contract/protocol_schemas/generated:stove0-observer/schemas/ObserverDescriptor`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:stove0-observer` — `reference/stove0/packages/observer-support/src/stove0_observer_support/schemas.py::observer_schema_bundle`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `contract_max` | maximum=120, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=200, minimum=1, reason=schema-maximum |

## Contract summary

- `title`: ObserverDescriptor
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `contracts` | yes | array |  |
| `descriptor_sha256` | yes | string |  |
| `image_digest` | yes | string |  |
| `implementation_id` | yes | string |  |
| `implementation_version` | yes | string |  |
| `protocol` | no | string |  |
| `source_revision` | yes | string |  |

### Definitions

| Definition | Shape |
|---|---|
| `JsonSchemaDocument` | object |
| `JsonValue` | object (0 fields) |
| `ObserverContractSupport` | object |
| `SemanticValidationProfile` | object |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5717766018649fca58b8d29b78adfc61b4a572a9a7d959ac997b6f9756460f41 -->

```json
{
  "$defs": {
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
    "ObserverContractSupport": {
      "additionalProperties": false,
      "properties": {
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
        "facts_schema": {
          "$ref": "#/$defs/JsonSchemaDocument"
        },
        "facts_semantics": {
          "$ref": "#/$defs/SemanticValidationProfile"
        },
        "maximum_result_bytes": {
          "maximum": 67108864,
          "minimum": 1,
          "title": "Maximum Result Bytes",
          "type": "integer"
        },
        "options_schema": {
          "$ref": "#/$defs/JsonSchemaDocument"
        },
        "preferred_subject_batch_size": {
          "default": 128,
          "minimum": 1,
          "title": "Preferred Subject Batch Size",
          "type": "integer"
        }
      },
      "required": [
        "contract_id",
        "contract_sha256",
        "options_schema",
        "facts_schema",
        "facts_semantics",
        "maximum_result_bytes"
      ],
      "title": "ObserverContractSupport",
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
    "contracts": {
      "items": {
        "$ref": "#/$defs/ObserverContractSupport"
      },
      "minItems": 1,
      "title": "Contracts",
      "type": "array"
    },
    "descriptor_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Descriptor Sha256",
      "type": "string"
    },
    "image_digest": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Image Digest",
      "type": "string"
    },
    "implementation_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Implementation Id",
      "type": "string"
    },
    "implementation_version": {
      "maxLength": 120,
      "minLength": 1,
      "title": "Implementation Version",
      "type": "string"
    },
    "protocol": {
      "const": "stove0-content-observer/v1",
      "default": "stove0-content-observer/v1",
      "title": "Protocol",
      "type": "string"
    },
    "source_revision": {
      "maxLength": 200,
      "minLength": 1,
      "title": "Source Revision",
      "type": "string"
    }
  },
  "required": [
    "implementation_id",
    "implementation_version",
    "source_revision",
    "image_digest",
    "contracts",
    "descriptor_sha256"
  ],
  "title": "ObserverDescriptor",
  "type": "object"
}
```
