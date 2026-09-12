# generated:stove0-review-sampler: SamplerResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:stove0-review-sampler-support:generated-stove0-review-sampler-samplerresult:84b113bb2c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-support](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](index.md#f-fba3a5663c) |
| Contract elements | 1 |
| Extent decisions | 5 |

## External contract

<a id="s-a445d3704c"></a>
- <a id="s-45045117a2"></a>`title`: SamplerResult
- <a id="s-85b7f4ac30"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e4192cb294"></a>`execution_evidence` | no | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-8f6d9f0d95"></a>`failure` | no | anyOf=#/$defs/SamplerFailure \| type="null" |  |
| <a id="s-bfd2e78f66"></a>`format` | no | type="string"; const="stove0-review-sampler-result/v1" |  |
| <a id="s-69ddaf5c63"></a>`inapplicable` | no | anyOf=#/$defs/SamplerInapplicable \| type="null" |  |
| <a id="s-d8dbe2a4a2"></a>`outputs` | no | type="array"; items=(#/$defs/SamplerOutput) |  |
| <a id="s-536576137a"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-598c04a1e2"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-eef522c17f"></a>`sampler_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4e4d0cd28d"></a>`state` | yes | type="string"; enum=["succeeded","inapplicable","failed","canceled"] |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-4394584684"></a>`JsonValue` | empty object |
| <a id="s-fc671c79af"></a>`SamplerFailure` | type="object"; fields=`code`, `message`, `retryable`; additional keys=`additionalProperties`, `required` |
| <a id="s-2a8e3a4769"></a>`SamplerInapplicable` | type="object"; fields=`code`, `message`; additional keys=`additionalProperties`, `required` |
| <a id="s-55b2cc4f6b"></a>`SamplerOutput` | type="object"; fields=`bytes`, `derived_from`, `id`, `media_type`, `path`, `sha256`; additional keys=`additionalProperties`, `required` |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-review-sampler-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field execution_evidence](#s-e4192cb294) | `cardinality · entries · operational_policy` | shared above |
| [field outputs](#s-d8dbe2a4a2) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field request_sha256](#s-536576137a) | `length · characters · fixed` | shared above |
| [field result_sha256](#s-598c04a1e2) | `length · characters · fixed` | shared above |
| [field sampler_descriptor_sha256](#s-eef522c17f) | `length · characters · fixed` | shared above |

## Governing policies

- <a id="pa-e20671a6f3"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-f6eaea6088"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-f8a6e2c97e"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:stove0-review-sampler](../../../evidence/sources.md#src-b47f3f4d7b) — `reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/schemas.py::sampler_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-review-sampler/schemas/SamplerResult`

### Exact owned JSON

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
