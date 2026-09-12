# generated:stove0-review-sampler: SamplerRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:stove0-review-sampler-support:generated-stove0-review-sampler-samplerrequest:afb432726c -->

| Audit field | Value |
|---|---|
| Authority | `stove0-review-sampler-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 9 |

## Machine authority

- `/external_contract/protocol_schemas/generated:stove0-review-sampler/schemas/SamplerRequest`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:stove0-review-sampler` — `reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/schemas.py::sampler_schema_bundle`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `contract_max` | maximum=4096, minimum=1, reason=schema-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `contract_max` | maximum=1099511627776, minimum=1, reason=schema-maximum |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| value | schema-value | `contract_max` | maximum=86400, minimum=1, reason=schema-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: SamplerRequest
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `cancellation_path` | yes | string |  |
| `format` | no | string |  |
| `inputs` | yes | array |  |
| `maximum_output_bytes` | yes | integer |  |
| `portable_intent` | yes | object |  |
| `request_sha256` | yes | string |  |
| `sampler_descriptor_sha256` | yes | string |  |
| `timeout_seconds` | yes | integer |  |
| `windows` | yes | array |  |
| `workspace_id` | yes | string |  |

### Definitions

| Definition | Shape |
|---|---|
| `JsonValue` | object (0 fields) |
| `SamplerInput` | object |
| `SamplerWindow` | object |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c4c0c1f6ca2eeb83c6b5bb7efc469587b92ba815ce556e33a976a98b0d093c9f -->

```json
{
  "$defs": {
    "JsonValue": {},
    "SamplerInput": {
      "additionalProperties": false,
      "properties": {
        "bytes": {
          "minimum": 0,
          "title": "Bytes",
          "type": "integer"
        },
        "id": {
          "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "media_type": {
          "anyOf": [
            {
              "maxLength": 255,
              "minLength": 1,
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Media Type"
        },
        "path": {
          "maxLength": 4096,
          "minLength": 1,
          "title": "Path",
          "type": "string"
        },
        "sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Sha256",
          "type": "string"
        }
      },
      "required": [
        "id",
        "path",
        "bytes",
        "sha256"
      ],
      "title": "SamplerInput",
      "type": "object"
    },
    "SamplerWindow": {
      "additionalProperties": false,
      "properties": {
        "duration_ms": {
          "minimum": 1,
          "title": "Duration Ms",
          "type": "integer"
        },
        "id": {
          "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "input_id": {
          "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
          "title": "Input Id",
          "type": "string"
        },
        "output_path": {
          "maxLength": 4096,
          "minLength": 1,
          "title": "Output Path",
          "type": "string"
        },
        "start_ms": {
          "minimum": 0,
          "title": "Start Ms",
          "type": "integer"
        }
      },
      "required": [
        "id",
        "input_id",
        "start_ms",
        "duration_ms",
        "output_path"
      ],
      "title": "SamplerWindow",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
    "cancellation_path": {
      "maxLength": 4096,
      "minLength": 1,
      "title": "Cancellation Path",
      "type": "string"
    },
    "format": {
      "const": "stove0-review-sampler-request/v1",
      "default": "stove0-review-sampler-request/v1",
      "title": "Format",
      "type": "string"
    },
    "inputs": {
      "items": {
        "$ref": "#/$defs/SamplerInput"
      },
      "minItems": 1,
      "title": "Inputs",
      "type": "array"
    },
    "maximum_output_bytes": {
      "maximum": 1099511627776,
      "minimum": 1,
      "title": "Maximum Output Bytes",
      "type": "integer"
    },
    "portable_intent": {
      "additionalProperties": {
        "$ref": "#/$defs/JsonValue"
      },
      "title": "Portable Intent",
      "type": "object"
    },
    "request_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Request Sha256",
      "type": "string"
    },
    "sampler_descriptor_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Sampler Descriptor Sha256",
      "type": "string"
    },
    "timeout_seconds": {
      "maximum": 86400,
      "minimum": 1,
      "title": "Timeout Seconds",
      "type": "integer"
    },
    "windows": {
      "items": {
        "$ref": "#/$defs/SamplerWindow"
      },
      "minItems": 1,
      "title": "Windows",
      "type": "array"
    },
    "workspace_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Workspace Id",
      "type": "string"
    }
  },
  "required": [
    "sampler_descriptor_sha256",
    "workspace_id",
    "inputs",
    "windows",
    "portable_intent",
    "maximum_output_bytes",
    "timeout_seconds",
    "cancellation_path",
    "request_sha256"
  ],
  "title": "SamplerRequest",
  "type": "object"
}
```
