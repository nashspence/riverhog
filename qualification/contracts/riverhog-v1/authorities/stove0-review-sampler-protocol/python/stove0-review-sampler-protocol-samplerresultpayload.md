# stove0_review_sampler_protocol.SamplerResultPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-protocol:stove0-review-sampler-protocol-samplerresultpayload:a978d5723e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b19295588e"></a>
- <a id="s-4b70d85692"></a>`distribution`: `stove0-review-sampler-protocol`
- <a id="s-3ccf3e8b68"></a>`module`: `stove0_review_sampler_protocol`
- <a id="s-79281dffe7"></a>`name`: `SamplerResultPayload`
- <a id="s-5a3cfc3f87"></a>`unit`: `export`

### Declared structure

- <a id="s-74ddeea582"></a>`kind`: `"class"`
- <a id="s-1cba412af6"></a>`signature`: `"\"(*, format: Literal['stove0-review-sampler-result/v1'] = 'stove0-review-sampler-result/v1', request_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], sampler_descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], state: Literal['succeeded', 'inapplicable', 'failed', 'canceled'], outputs: tuple[stove0_review_sampler_protocol.SamplerOutput, ...] = (), execution_evidence: dict[str, JsonValue] = <factory>, failure: stove0_review_sampler_protocol.SamplerFailure \| None = None, inapplicable: stove0_review_sampler_protocol.SamplerInapplicable \| None = None) -> None\""`

#### Validated model schema

<a id="s-d4ec280a21"></a>

- <a id="s-d610b6e916"></a>`type`: `"object"`
- <a id="s-1d70f71db9"></a>`additionalProperties`: `false`
- <a id="s-c0bcab908c"></a>`required`: `["request_sha256","sampler_descriptor_sha256","state"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c19d683b2d"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-79443c3733)) |  |
| <a id="s-0d2aa44490"></a>`failure` | no | anyOf=[([SamplerFailure](#s-82b59a94c6)); (type="null")]; default=null |  |
| <a id="s-20ed96c908"></a>`format` | no | type="string"; const="stove0-review-sampler-result/v1"; default="stove0-review-sampler-result/v1" |  |
| <a id="s-5f40521f3b"></a>`inapplicable` | no | anyOf=[([SamplerInapplicable](#s-8e2a7dcc74)); (type="null")]; default=null |  |
| <a id="s-5bfb3df8b3"></a>`outputs` | no | type="array"; default=[]; items=([SamplerOutput](#s-2c0690b424)) |  |
| <a id="s-7a462fb9ba"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-fc7faa8e8f"></a>`sampler_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-76a98465b3"></a>`state` | yes | type="string"; enum=["succeeded","inapplicable","failed","canceled"] |  |

##### Definitions

- [JsonValue](#s-79443c3733)
- [SamplerFailure](#s-82b59a94c6)
- [SamplerInapplicable](#s-8e2a7dcc74)
- [SamplerOutput](#s-2c0690b424)

##### <a id="s-79443c3733"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-82b59a94c6"></a>definition `SamplerFailure`

- <a id="s-cd99840259"></a>`type`: `"object"`
- <a id="s-acb1f82a45"></a>`additionalProperties`: `false`
- <a id="s-101284621a"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8ba8a554b9"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-c576dc9065"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-8da992309a"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-8e2a7dcc74"></a>definition `SamplerInapplicable`

- <a id="s-2cada6dad7"></a>`type`: `"object"`
- <a id="s-0149fdcceb"></a>`additionalProperties`: `false`
- <a id="s-b64e83717e"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4463d3e98d"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-9bc375b551"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-2c0690b424"></a>definition `SamplerOutput`

- <a id="s-ddefa775e5"></a>`type`: `"object"`
- <a id="s-9100d3f1f9"></a>`additionalProperties`: `false`
- <a id="s-27386a5aed"></a>`required`: `["id","path","bytes","sha256","media_type","derived_from"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-09f8c4b061"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-fe722ef7ab"></a>`derived_from` | yes | type="array"; items=(type="string"); minItems=1 |  |
| <a id="s-6346e05497"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-3bcd54a17e"></a>`media_type` | yes | type="string"; maxLength=255; minLength=1 |  |
| <a id="s-301b5449f5"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-8193fdc943"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [state_shape](stove0-review-sampler-protocol-samplerresultpayload-state-shape.md)
- [canonical_outputs](stove0-review-sampler-protocol-samplerresultpayload-canonical-outputs.md)

## Governing policies

- <a id="pa-e3228be7cc"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../../../evidence/sources/authorities.md#src-25c43eb779) — [reference/stove0/targets/review/sampler/protocol/src/stove0\_review\_sampler\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/sampler/protocol/src/stove0_review_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_sampler_protocol.SamplerResultPayload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a52999bfdf70a4df1b953386d83f93477c470c15ba8a89f67ccd04770838295d -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "JsonValue": {},
        "SamplerFailure": {
          "additionalProperties": false,
          "properties": {
            "code": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "message": {
              "maxLength": 1000,
              "minLength": 1,
              "type": "string"
            },
            "retryable": {
              "type": "boolean"
            }
          },
          "required": [
            "code",
            "message",
            "retryable"
          ],
          "type": "object"
        },
        "SamplerInapplicable": {
          "additionalProperties": false,
          "properties": {
            "code": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "message": {
              "maxLength": 1000,
              "minLength": 1,
              "type": "string"
            }
          },
          "required": [
            "code",
            "message"
          ],
          "type": "object"
        },
        "SamplerOutput": {
          "additionalProperties": false,
          "properties": {
            "bytes": {
              "minimum": 0,
              "type": "integer"
            },
            "derived_from": {
              "items": {
                "type": "string"
              },
              "minItems": 1,
              "type": "array"
            },
            "id": {
              "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
              "type": "string"
            },
            "media_type": {
              "maxLength": 255,
              "minLength": 1,
              "type": "string"
            },
            "path": {
              "maxLength": 4096,
              "minLength": 1,
              "type": "string"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
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
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "execution_evidence": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
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
          "type": "array"
        },
        "request_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "sampler_descriptor_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "state": {
          "enum": [
            "succeeded",
            "inapplicable",
            "failed",
            "canceled"
          ],
          "type": "string"
        }
      },
      "required": [
        "request_sha256",
        "sampler_descriptor_sha256",
        "state"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-review-sampler-result/v1'] = 'stove0-review-sampler-result/v1', request_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], sampler_descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], state: Literal['succeeded', 'inapplicable', 'failed', 'canceled'], outputs: tuple[stove0_review_sampler_protocol.SamplerOutput, ...] = (), execution_evidence: dict[str, JsonValue] = <factory>, failure: stove0_review_sampler_protocol.SamplerFailure | None = None, inapplicable: stove0_review_sampler_protocol.SamplerInapplicable | None = None) -> None\""
  },
  "distribution": "stove0-review-sampler-protocol",
  "module": "stove0_review_sampler_protocol",
  "name": "SamplerResultPayload",
  "unit": "export"
}
```

</details>
