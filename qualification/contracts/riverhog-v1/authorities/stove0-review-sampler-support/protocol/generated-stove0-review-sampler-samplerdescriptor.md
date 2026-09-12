# generated:stove0-review-sampler: SamplerDescriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:stove0-review-sampler-support:generated-stove0-review-sampler-samplerdescriptor:e7f5e3e5e8 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-review-sampler-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 5 |

## Machine authority

- `/external_contract/protocol_schemas/generated:stove0-review-sampler/schemas/SamplerDescriptor`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:stove0-review-sampler` — `reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/schemas.py::sampler_schema_bundle`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `contract_max` | maximum=120, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `contract_max` | maximum=200, minimum=1, reason=schema-maximum |

## Contract summary

- `title`: SamplerDescriptor
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `descriptor_sha256` | yes | string |  |
| `image_digest` | yes | string |  |
| `implementation_id` | yes | string |  |
| `implementation_version` | yes | string |  |
| `output_role` | yes | string |  |
| `portable_intent_schema` | yes | #/$defs/JsonSchemaDocument |  |
| `primary_operation_contract_sha256` | yes | string |  |
| `primary_operation_id` | yes | string |  |
| `protocol` | no | string |  |
| `source_revision` | yes | string |  |

### Definitions

| Definition | Shape |
|---|---|
| `JsonSchemaDocument` | object |
| `JsonValue` | object (0 fields) |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1f0d4b42d3d84843e4fc3419d4ad90570af2991916b068a4f7658fc9cd25b077 -->

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
    "JsonValue": {}
  },
  "additionalProperties": false,
  "properties": {
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
    "output_role": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Output Role",
      "type": "string"
    },
    "portable_intent_schema": {
      "$ref": "#/$defs/JsonSchemaDocument"
    },
    "primary_operation_contract_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Primary Operation Contract Sha256",
      "type": "string"
    },
    "primary_operation_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Primary Operation Id",
      "type": "string"
    },
    "protocol": {
      "const": "stove0-review-sampler/v1",
      "default": "stove0-review-sampler/v1",
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
    "primary_operation_id",
    "primary_operation_contract_sha256",
    "portable_intent_schema",
    "output_role",
    "descriptor_sha256"
  ],
  "title": "SamplerDescriptor",
  "type": "object"
}
```
