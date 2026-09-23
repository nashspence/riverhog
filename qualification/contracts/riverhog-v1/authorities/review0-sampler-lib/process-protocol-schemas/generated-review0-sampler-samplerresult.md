# generated:review0-sampler: SamplerResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:review0-sampler-lib:generated-review0-sampler-samplerresult:61cabfea4f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-lib](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-07c4cb8915"></a>

- <a id="s-b29804915b"></a>`type`: `"object"`
- <a id="s-91e1fe24b8"></a>`additionalProperties`: `false`
- <a id="s-d55901c188"></a>`required`: `["request_sha256","sampler_descriptor_sha256","state","result_sha256"]`
- <a id="s-754bfe77e0"></a>`title`: `"SamplerResult"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0bb917e254"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-d5d187f50e)); title="Execution Evidence" |  |
| <a id="s-3af351dc05"></a>`failure` | no | anyOf=[([SamplerFailure](#s-02099ededb)); (type="null")]; default=null |  |
| <a id="s-b753047c8e"></a>`format` | no | type="string"; const="review0-sampler-result/v1"; default="review0-sampler-result/v1"; title="Format" |  |
| <a id="s-10b145a8fa"></a>`inapplicable` | no | anyOf=[([SamplerInapplicable](#s-1292d87f5e)); (type="null")]; default=null |  |
| <a id="s-12375f2559"></a>`outputs` | no | type="array"; default=[]; items=([SamplerOutput](#s-8f0a74d149)); title="Outputs" |  |
| <a id="s-36cff94acd"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Sha256" |  |
| <a id="s-f14e7334b8"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Result Sha256" |  |
| <a id="s-5f62e985bf"></a>`sampler_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sampler Descriptor Sha256" |  |
| <a id="s-fddfff93ba"></a>`state` | yes | type="string"; enum=["succeeded","inapplicable","failed","canceled"]; title="State" |  |

### Definitions

- [JsonValue](#s-d5d187f50e)
- [SamplerFailure](#s-02099ededb)
- [SamplerInapplicable](#s-1292d87f5e)
- [SamplerOutput](#s-8f0a74d149)

### <a id="s-d5d187f50e"></a>definition `JsonValue`

- Accepts: any JSON value.

### <a id="s-02099ededb"></a>definition `SamplerFailure`

- <a id="s-9d0408c97a"></a>`type`: `"object"`
- <a id="s-2cac239712"></a>`additionalProperties`: `false`
- <a id="s-74c4d922fb"></a>`required`: `["code","message","retryable"]`
- <a id="s-569012ba7e"></a>`title`: `"SamplerFailure"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8f5f1240b4"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-5eac17768f"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |
| <a id="s-a0d3fbc35a"></a>`retryable` | yes | type="boolean"; title="Retryable" |  |

### <a id="s-1292d87f5e"></a>definition `SamplerInapplicable`

- <a id="s-5ad4b0172b"></a>`type`: `"object"`
- <a id="s-7c2b81ecbd"></a>`additionalProperties`: `false`
- <a id="s-9b88cef177"></a>`required`: `["code","message"]`
- <a id="s-e891ba731f"></a>`title`: `"SamplerInapplicable"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-87884e2aec"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-8d8bebb516"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |

### <a id="s-8f0a74d149"></a>definition `SamplerOutput`

- <a id="s-4c1bbb7684"></a>`type`: `"object"`
- <a id="s-3c96f20036"></a>`additionalProperties`: `false`
- <a id="s-a545fd53a2"></a>`required`: `["id","path","bytes","sha256","media_type","derived_from"]`
- <a id="s-874e5d947f"></a>`title`: `"SamplerOutput"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-499d3e905d"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-7e61584d5b"></a>`derived_from` | yes | type="array"; items=(type="string"); minItems=1; title="Derived From" |  |
| <a id="s-95ed6dd681"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"; title="Id" |  |
| <a id="s-4bc6fde263"></a>`media_type` | yes | type="string"; maxLength=255; minLength=1; title="Media Type" |  |
| <a id="s-519a19d3de"></a>`path` | yes | type="string"; maxLength=4096; minLength=1; title="Path" |  |
| <a id="s-07e6c28fc6"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"review0-sampler-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field execution_evidence](#s-0bb917e254) | `cardinality · entries · operational_policy` | shared above |
| [field outputs](#s-12375f2559) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field request_sha256](#s-36cff94acd) | `length · characters · fixed` | shared above |
| [field result_sha256](#s-f14e7334b8) | `length · characters · fixed` | shared above |
| [field sampler_descriptor_sha256](#s-5f62e985bf) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [generated:review0-sampler protocol](../process-protocol/generated-review0-sampler-protocol.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-b9f1688239"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-4537532a02"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-3e8bf243b3"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:review0-sampler](../../../evidence/sources/authorities.md#src-8a54180841) — [some-implementations/stove0/review0/sampler/support/src/review0\_sampler\_lib/schemas.py::sampler\_schema\_bundle](../../../../../../some-implementations/stove0/review0/sampler/support/src/review0_sampler_lib/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:review0-sampler/schemas/SamplerResult`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c02e97a1b4332926d1e783113add6b0e2feef811434ed7176fd45c0da5c70097 -->

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
      "const": "review0-sampler-result/v1",
      "default": "review0-sampler-result/v1",
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
