# generated:stove0-review-sampler: SamplerResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:stove0-review-sampler-support:generated-stove0-review-sampler-samplerresult:ea1c027bc2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-a445d3704c"></a>

- <a id="s-85b7f4ac30"></a>`type`: `"object"`
- <a id="s-be89c57c6b"></a>`additionalProperties`: `false`
- <a id="s-3303134d21"></a>`required`: `["request_sha256","sampler_descriptor_sha256","state","result_sha256"]`
- <a id="s-45045117a2"></a>`title`: `"SamplerResult"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e4192cb294"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-4394584684)); title="Execution Evidence" |  |
| <a id="s-8f6d9f0d95"></a>`failure` | no | anyOf=[([SamplerFailure](#s-fc671c79af)); (type="null")]; default=null |  |
| <a id="s-bfd2e78f66"></a>`format` | no | type="string"; const="stove0-review-sampler-result/v1"; default="stove0-review-sampler-result/v1"; title="Format" |  |
| <a id="s-69ddaf5c63"></a>`inapplicable` | no | anyOf=[([SamplerInapplicable](#s-2a8e3a4769)); (type="null")]; default=null |  |
| <a id="s-d8dbe2a4a2"></a>`outputs` | no | type="array"; default=[]; items=([SamplerOutput](#s-55b2cc4f6b)); title="Outputs" |  |
| <a id="s-536576137a"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Sha256" |  |
| <a id="s-598c04a1e2"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Result Sha256" |  |
| <a id="s-eef522c17f"></a>`sampler_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sampler Descriptor Sha256" |  |
| <a id="s-4e4d0cd28d"></a>`state` | yes | type="string"; enum=["succeeded","inapplicable","failed","canceled"]; title="State" |  |

### Definitions

- [JsonValue](#s-4394584684)
- [SamplerFailure](#s-fc671c79af)
- [SamplerInapplicable](#s-2a8e3a4769)
- [SamplerOutput](#s-55b2cc4f6b)

### <a id="s-4394584684"></a>definition `JsonValue`

- Accepts: any JSON value.

### <a id="s-fc671c79af"></a>definition `SamplerFailure`

- <a id="s-c1cd7f23c9"></a>`type`: `"object"`
- <a id="s-1f43ce152a"></a>`additionalProperties`: `false`
- <a id="s-c6c72a23ac"></a>`required`: `["code","message","retryable"]`
- <a id="s-c00aa47f41"></a>`title`: `"SamplerFailure"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-99bdf3fbe0"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-60f7b02b04"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |
| <a id="s-1544ef6d15"></a>`retryable` | yes | type="boolean"; title="Retryable" |  |

### <a id="s-2a8e3a4769"></a>definition `SamplerInapplicable`

- <a id="s-e3eda523d7"></a>`type`: `"object"`
- <a id="s-3c21f79367"></a>`additionalProperties`: `false`
- <a id="s-4c071878fe"></a>`required`: `["code","message"]`
- <a id="s-84595cf1ba"></a>`title`: `"SamplerInapplicable"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b6bd901bb7"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-d6a8aac343"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |

### <a id="s-55b2cc4f6b"></a>definition `SamplerOutput`

- <a id="s-e7d8f66ed6"></a>`type`: `"object"`
- <a id="s-9f977a6d6e"></a>`additionalProperties`: `false`
- <a id="s-97d45dcfd3"></a>`required`: `["id","path","bytes","sha256","media_type","derived_from"]`
- <a id="s-38802795f0"></a>`title`: `"SamplerOutput"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-428a1ed582"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-d843eec98e"></a>`derived_from` | yes | type="array"; items=(type="string"); minItems=1; title="Derived From" |  |
| <a id="s-d8c8503ae6"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"; title="Id" |  |
| <a id="s-07d7974e71"></a>`media_type` | yes | type="string"; maxLength=255; minLength=1; title="Media Type" |  |
| <a id="s-f8e3536ede"></a>`path` | yes | type="string"; maxLength=4096; minLength=1; title="Path" |  |
| <a id="s-3ecee0330f"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

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

## Maintained corroboration

### Related interface records

- [generated:stove0-review-sampler protocol](../process-protocol/generated-stove0-review-sampler-protocol.md)

## Governing policies

- <a id="pa-2ac5a34a95"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-5cdfdc424e"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-67751b86e1"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:stove0-review-sampler](../../../evidence/sources.md#src-b47f3f4d7b) — [reference/stove0/targets/review/sampler/support/src/stove0\_review\_sampler\_support/schemas.py::sampler\_schema\_bundle](../../../../../../reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-review-sampler/schemas/SamplerResult`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
