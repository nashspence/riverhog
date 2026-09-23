# generated:review0-sampler: SamplerRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:review0-sampler-lib:generated-review0-sampler-samplerrequest:a2fdadfaa3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-lib](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-3288e6beec"></a>

- <a id="s-97c36ae901"></a>`type`: `"object"`
- <a id="s-f3c6688f94"></a>`additionalProperties`: `false`
- <a id="s-6d7c9ad129"></a>`required`: `["sampler_descriptor_sha256","workspace_id","inputs","windows","portable_intent","maximum_output_bytes","timeout_seconds","cancellation_path","request_sha256"]`
- <a id="s-6de54ee226"></a>`title`: `"SamplerRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6f21464863"></a>`cancellation_path` | yes | type="string"; maxLength=4096; minLength=1; title="Cancellation Path" |  |
| <a id="s-7bb4235d07"></a>`format` | no | type="string"; const="review0-sampler-request/v1"; default="review0-sampler-request/v1"; title="Format" |  |
| <a id="s-36188e986a"></a>`inputs` | yes | type="array"; items=([SamplerInput](#s-67d8ae240a)); minItems=1; title="Inputs" |  |
| <a id="s-17c57c7dde"></a>`maximum_output_bytes` | yes | type="integer"; minimum=1; maximum=1099511627776; title="Maximum Output Bytes" |  |
| <a id="s-069208c230"></a>`portable_intent` | yes | type="object"; additionalProperties=([JsonValue](#s-9c72a56dba)); title="Portable Intent" |  |
| <a id="s-a18573603b"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Sha256" |  |
| <a id="s-438f9b66da"></a>`sampler_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sampler Descriptor Sha256" |  |
| <a id="s-c84c4fd6f2"></a>`timeout_seconds` | yes | type="integer"; minimum=1; maximum=86400; title="Timeout Seconds" |  |
| <a id="s-21b277905d"></a>`windows` | yes | type="array"; items=([SamplerWindow](#s-e40148e3aa)); minItems=1; title="Windows" |  |
| <a id="s-5f7a8e876d"></a>`workspace_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Workspace Id" |  |

### Definitions

- [JsonValue](#s-9c72a56dba)
- [SamplerInput](#s-67d8ae240a)
- [SamplerWindow](#s-e40148e3aa)

### <a id="s-9c72a56dba"></a>definition `JsonValue`

- Accepts: any JSON value.

### <a id="s-67d8ae240a"></a>definition `SamplerInput`

- <a id="s-f1eb93c964"></a>`type`: `"object"`
- <a id="s-17f4eac70e"></a>`additionalProperties`: `false`
- <a id="s-ac34d58084"></a>`required`: `["id","path","bytes","sha256"]`
- <a id="s-cade57425c"></a>`title`: `"SamplerInput"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-88f5dd3446"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-df2506ee45"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"; title="Id" |  |
| <a id="s-8f4c6942ac"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null; title="Media Type" |  |
| <a id="s-36ed50328a"></a>`path` | yes | type="string"; maxLength=4096; minLength=1; title="Path" |  |
| <a id="s-c0a9ec6bff"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### <a id="s-e40148e3aa"></a>definition `SamplerWindow`

- <a id="s-1126d55a3c"></a>`type`: `"object"`
- <a id="s-2fe8f5869b"></a>`additionalProperties`: `false`
- <a id="s-8bb5c9ace1"></a>`required`: `["id","input_id","start_ms","duration_ms","output_path"]`
- <a id="s-23592dfc15"></a>`title`: `"SamplerWindow"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c2ed174a9b"></a>`duration_ms` | yes | type="integer"; minimum=1; title="Duration Ms" |  |
| <a id="s-48e689f130"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"; title="Id" |  |
| <a id="s-0b11c7dc12"></a>`input_id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"; title="Input Id" |  |
| <a id="s-8e99258315"></a>`output_path` | yes | type="string"; maxLength=4096; minLength=1; title="Output Path" |  |
| <a id="s-07a5bc3684"></a>`start_ms` | yes | type="integer"; minimum=0; title="Start Ms" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"review0-sampler-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field inputs](#s-36188e986a) | `cardinality · items · operational_policy` | shared above |
| [field portable_intent](#s-069208c230) | `cardinality · entries · operational_policy` | shared above |
| [field windows](#s-21b277905d) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field cancellation_path](#s-6f21464863) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| [field maximum_output_bytes](#s-17c57c7dde) | `value · schema-value · contract_max` | maximum=1099511627776; minimum=1; reason="schema-maximum" |
| [field request_sha256](#s-a18573603b) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field sampler_descriptor_sha256](#s-438f9b66da) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field timeout_seconds](#s-c84c4fd6f2) | `value · schema-value · contract_max` | maximum=86400; minimum=1; reason="schema-maximum" |
| [field workspace_id](#s-5f7a8e876d) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Related interface records

- [generated:review0-sampler protocol](../process-protocol/generated-review0-sampler-protocol.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-fb7e863ec7"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-312e54bfb0"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-6e93bf6bf8"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:review0-sampler](../../../evidence/sources/authorities.md#src-8a54180841) — [some-implementations/stove0/review0/sampler/support/src/review0\_sampler\_lib/schemas.py::sampler\_schema\_bundle](../../../../../../some-implementations/stove0/review0/sampler/support/src/review0_sampler_lib/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:review0-sampler/schemas/SamplerRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 48b9a6ca0f8af410b919bd9c4802f3b3881be0b164ead0cc5ae59295218af82c -->

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
      "const": "review0-sampler-request/v1",
      "default": "review0-sampler-request/v1",
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
