# generated:stove0-review-sampler: SamplerRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:stove0-review-sampler-support:generated-stove0-review-sampler-samplerrequest:5fde579bb5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-3bd9a7033e"></a>

- <a id="s-a6e6ae76a0"></a>`type`: `"object"`
- <a id="s-5fe06e7d8c"></a>`additionalProperties`: `false`
- <a id="s-7b4880c9cb"></a>`required`: `["sampler_descriptor_sha256","workspace_id","inputs","windows","portable_intent","maximum_output_bytes","timeout_seconds","cancellation_path","request_sha256"]`
- <a id="s-8f62b2d8a7"></a>`title`: `"SamplerRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a8f65c3c76"></a>`cancellation_path` | yes | type="string"; maxLength=4096; minLength=1; title="Cancellation Path" |  |
| <a id="s-43fbda1947"></a>`format` | no | type="string"; const="stove0-review-sampler-request/v1"; default="stove0-review-sampler-request/v1"; title="Format" |  |
| <a id="s-a2bae7da3c"></a>`inputs` | yes | type="array"; items=([SamplerInput](#s-95bb3b80ca)); minItems=1; title="Inputs" |  |
| <a id="s-3c818a2845"></a>`maximum_output_bytes` | yes | type="integer"; minimum=1; maximum=1099511627776; title="Maximum Output Bytes" |  |
| <a id="s-d2dff3997e"></a>`portable_intent` | yes | type="object"; additionalProperties=([JsonValue](#s-3d327bc4bb)); title="Portable Intent" |  |
| <a id="s-8773e89c7f"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Sha256" |  |
| <a id="s-8b194292eb"></a>`sampler_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sampler Descriptor Sha256" |  |
| <a id="s-eccbfccaf9"></a>`timeout_seconds` | yes | type="integer"; minimum=1; maximum=86400; title="Timeout Seconds" |  |
| <a id="s-7d6207dbac"></a>`windows` | yes | type="array"; items=([SamplerWindow](#s-4b7a08db4c)); minItems=1; title="Windows" |  |
| <a id="s-d68872f84a"></a>`workspace_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Workspace Id" |  |

### Definitions

- [JsonValue](#s-3d327bc4bb)
- [SamplerInput](#s-95bb3b80ca)
- [SamplerWindow](#s-4b7a08db4c)

### <a id="s-3d327bc4bb"></a>definition `JsonValue`

- Accepts: any JSON value.

### <a id="s-95bb3b80ca"></a>definition `SamplerInput`

- <a id="s-b9c5d6fc00"></a>`type`: `"object"`
- <a id="s-398e2321af"></a>`additionalProperties`: `false`
- <a id="s-9a56ab2649"></a>`required`: `["id","path","bytes","sha256"]`
- <a id="s-4c75d0c537"></a>`title`: `"SamplerInput"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-419b80f7be"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-bee0427013"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"; title="Id" |  |
| <a id="s-c8172787c7"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null; title="Media Type" |  |
| <a id="s-5dbf680d03"></a>`path` | yes | type="string"; maxLength=4096; minLength=1; title="Path" |  |
| <a id="s-8847f1498b"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### <a id="s-4b7a08db4c"></a>definition `SamplerWindow`

- <a id="s-a987487f85"></a>`type`: `"object"`
- <a id="s-ce3d43270a"></a>`additionalProperties`: `false`
- <a id="s-7c20d8d525"></a>`required`: `["id","input_id","start_ms","duration_ms","output_path"]`
- <a id="s-778d3bd322"></a>`title`: `"SamplerWindow"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e87731674b"></a>`duration_ms` | yes | type="integer"; minimum=1; title="Duration Ms" |  |
| <a id="s-4e43e5a835"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"; title="Id" |  |
| <a id="s-c0ea96528f"></a>`input_id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"; title="Input Id" |  |
| <a id="s-38c9979cf1"></a>`output_path` | yes | type="string"; maxLength=4096; minLength=1; title="Output Path" |  |
| <a id="s-265ff94202"></a>`start_ms` | yes | type="integer"; minimum=0; title="Start Ms" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-review-sampler-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field inputs](#s-a2bae7da3c) | `cardinality · items · operational_policy` | shared above |
| [field portable_intent](#s-d2dff3997e) | `cardinality · entries · operational_policy` | shared above |
| [field windows](#s-7d6207dbac) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field cancellation_path](#s-a8f65c3c76) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| [field maximum_output_bytes](#s-3c818a2845) | `value · schema-value · contract_max` | maximum=1099511627776; minimum=1; reason="schema-maximum" |
| [field request_sha256](#s-8773e89c7f) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field sampler_descriptor_sha256](#s-8b194292eb) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field timeout_seconds](#s-eccbfccaf9) | `value · schema-value · contract_max` | maximum=86400; minimum=1; reason="schema-maximum" |
| [field workspace_id](#s-d68872f84a) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Related interface records

- [generated:stove0-review-sampler protocol](../process-protocol/generated-stove0-review-sampler-protocol.md)

## Governing policies

- <a id="pa-cac643af59"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-3e4cc643b6"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-c3cb5e3d84"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:stove0-review-sampler](../../../evidence/sources.md#src-b47f3f4d7b) — `reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/schemas.py::sampler_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-review-sampler/schemas/SamplerRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
