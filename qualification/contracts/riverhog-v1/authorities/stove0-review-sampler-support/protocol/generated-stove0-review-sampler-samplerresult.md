# generated:stove0-review-sampler: SamplerResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:stove0-review-sampler-support:generated-stove0-review-sampler-samplerresult:84b113bb2c -->

| Audit field | Value |
|---|---|
| Authority | `stove0-review-sampler-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 5 |

## Machine authority

- `/external_contract/protocol_schemas/generated:stove0-review-sampler/schemas/SamplerResult`

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
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: SamplerResult
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `execution_evidence` | no | object |  |
| `failure` | no | object (2 fields) |  |
| `format` | no | string |  |
| `inapplicable` | no | object (2 fields) |  |
| `outputs` | no | array |  |
| `request_sha256` | yes | string |  |
| `result_sha256` | yes | string |  |
| `sampler_descriptor_sha256` | yes | string |  |
| `state` | yes | string |  |

### Definitions

| Definition | Shape |
|---|---|
| `JsonValue` | object (0 fields) |
| `SamplerFailure` | object |
| `SamplerInapplicable` | object |
| `SamplerOutput` | object |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 21f6b9d3f23551988794e5c55d6db07545900c3d9b5dfc7b2372acc553d8e0d3 -->

```json
{
  "$defs": {
    "JsonValue": {},
    "SamplerFailure": {
      "additionalProperties": false,
      "properties": {
        "code": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Code",
          "type": "string"
        },
        "message": {
          "maxLength": 1000,
          "minLength": 1,
          "title": "Message",
          "type": "string"
        },
        "retryable": {
          "title": "Retryable",
          "type": "boolean"
        }
      },
      "required": [
        "code",
        "message",
        "retryable"
      ],
      "title": "SamplerFailure",
      "type": "object"
    },
    "SamplerInapplicable": {
      "additionalProperties": false,
      "properties": {
        "code": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Code",
          "type": "string"
        },
        "message": {
          "maxLength": 1000,
          "minLength": 1,
          "title": "Message",
          "type": "string"
        }
      },
      "required": [
        "code",
        "message"
      ],
      "title": "SamplerInapplicable",
      "type": "object"
    },
    "SamplerOutput": {
      "additionalProperties": false,
      "properties": {
        "bytes": {
          "minimum": 0,
          "title": "Bytes",
          "type": "integer"
        },
        "derived_from": {
          "items": {
            "type": "string"
          },
          "minItems": 1,
          "title": "Derived From",
          "type": "array"
        },
        "id": {
          "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "media_type": {
          "maxLength": 255,
          "minLength": 1,
          "title": "Media Type",
          "type": "string"
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
        "sha256",
        "media_type",
        "derived_from"
      ],
      "title": "SamplerOutput",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
    "execution_evidence": {
      "additionalProperties": {
        "$ref": "#/$defs/JsonValue"
      },
      "title": "Execution Evidence",
      "type": "object"
    },
    "failure": {
      "anyOf": [
        {
          "$ref": "#/$defs/SamplerFailure"
        },
        {
          "type": "null"
        }
      ],
      "default": null
    },
    "format": {
      "const": "stove0-review-sampler-result/v1",
      "default": "stove0-review-sampler-result/v1",
      "title": "Format",
      "type": "string"
    },
    "inapplicable": {
      "anyOf": [
        {
          "$ref": "#/$defs/SamplerInapplicable"
        },
        {
          "type": "null"
        }
      ],
      "default": null
    },
    "outputs": {
      "default": [],
      "items": {
        "$ref": "#/$defs/SamplerOutput"
      },
      "title": "Outputs",
      "type": "array"
    },
    "request_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Request Sha256",
      "type": "string"
    },
    "result_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Result Sha256",
      "type": "string"
    },
    "sampler_descriptor_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Sampler Descriptor Sha256",
      "type": "string"
    },
    "state": {
      "enum": [
        "succeeded",
        "inapplicable",
        "failed",
        "canceled"
      ],
      "title": "State",
      "type": "string"
    }
  },
  "required": [
    "request_sha256",
    "sampler_descriptor_sha256",
    "state",
    "result_sha256"
  ],
  "title": "SamplerResult",
  "type": "object"
}
```
