# review0_sampler_protocol.SamplerResultPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-protocol:review0-sampler-protocol-samplerresultpayload:df9d4a7389 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f99b353220"></a>
- <a id="s-3459558353"></a>`distribution`: `review0-sampler-protocol`
- <a id="s-0de8150ad6"></a>`module`: `review0_sampler_protocol`
- <a id="s-05e2a78b6d"></a>`name`: `SamplerResultPayload`
- <a id="s-8bb7fbe5b1"></a>`unit`: `export`

### Declared structure

- <a id="s-39f8f63c7f"></a>`kind`: `"class"`
- <a id="s-f4b1a0ecb4"></a>`signature`: `"\"(*, format: Literal['review0-sampler-result/v1'] = 'review0-sampler-result/v1', request_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], sampler_descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], state: Literal['succeeded', 'inapplicable', 'failed', 'canceled'], outputs: tuple[review0_sampler_protocol.SamplerOutput, ...] = (), execution_evidence: dict[str, JsonValue] = <factory>, failure: review0_sampler_protocol.SamplerFailure \| None = None, inapplicable: review0_sampler_protocol.SamplerInapplicable \| None = None) -> None\""`

#### Validated model schema

<a id="s-daa96d00d0"></a>

- <a id="s-bd403110a8"></a>`type`: `"object"`
- <a id="s-7f18ba7f0d"></a>`additionalProperties`: `false`
- <a id="s-35b20e0992"></a>`required`: `["request_sha256","sampler_descriptor_sha256","state"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1cb14f5c23"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-deea46e8a0)) |  |
| <a id="s-9309907232"></a>`failure` | no | anyOf=[([SamplerFailure](#s-f812ddcde8)); (type="null")]; default=null |  |
| <a id="s-75df196826"></a>`format` | no | type="string"; const="review0-sampler-result/v1"; default="review0-sampler-result/v1" |  |
| <a id="s-0764d405f2"></a>`inapplicable` | no | anyOf=[([SamplerInapplicable](#s-556e599f0b)); (type="null")]; default=null |  |
| <a id="s-10e4093bc0"></a>`outputs` | no | type="array"; default=[]; items=([SamplerOutput](#s-99e65ee3a0)) |  |
| <a id="s-8a12ef52af"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c190fc3970"></a>`sampler_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5d943647d0"></a>`state` | yes | type="string"; enum=["succeeded","inapplicable","failed","canceled"] |  |

##### Definitions

- [JsonValue](#s-deea46e8a0)
- [SamplerFailure](#s-f812ddcde8)
- [SamplerInapplicable](#s-556e599f0b)
- [SamplerOutput](#s-99e65ee3a0)

##### <a id="s-deea46e8a0"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-f812ddcde8"></a>definition `SamplerFailure`

- <a id="s-ddf01fec12"></a>`type`: `"object"`
- <a id="s-765e58d78f"></a>`additionalProperties`: `false`
- <a id="s-e37a1d7fa3"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7debecb068"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-266506bfba"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-29ff3cd2ed"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-556e599f0b"></a>definition `SamplerInapplicable`

- <a id="s-66fdf80226"></a>`type`: `"object"`
- <a id="s-865c0d5618"></a>`additionalProperties`: `false`
- <a id="s-5df3c87d18"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c71488ba1d"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-9182ebb29f"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-99e65ee3a0"></a>definition `SamplerOutput`

- <a id="s-76fceb5486"></a>`type`: `"object"`
- <a id="s-705168d748"></a>`additionalProperties`: `false`
- <a id="s-b7e73dc4be"></a>`required`: `["id","path","bytes","sha256","media_type","derived_from"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1fe1a8134e"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-5ee57e2785"></a>`derived_from` | yes | type="array"; items=(type="string"); minItems=1 |  |
| <a id="s-002ed028dc"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-e35c94a510"></a>`media_type` | yes | type="string"; maxLength=255; minLength=1 |  |
| <a id="s-be214f163f"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-29c87fb205"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [state_shape](review0-sampler-protocol-samplerresultpayload-state-shape.md)
- [canonical_outputs](review0-sampler-protocol-samplerresultpayload-canonical-outputs.md)

## Governing policies

- <a id="pa-4d0a64d32c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-protocol:review0_sampler_protocol](../../../evidence/sources/authorities.md#src-317a05ab5e) — [some-implementations/stove0/review0/sampler/protocol/src/review0\_sampler\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/protocol/src/review0_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_protocol.SamplerResultPayload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8a3f9b5b562a17c823bd72f8db28422fde27eecc480be0ff5de673dd5da2a794 -->

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
          "const": "review0-sampler-result/v1",
          "default": "review0-sampler-result/v1",
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
    "signature": "\"(*, format: Literal['review0-sampler-result/v1'] = 'review0-sampler-result/v1', request_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], sampler_descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], state: Literal['succeeded', 'inapplicable', 'failed', 'canceled'], outputs: tuple[review0_sampler_protocol.SamplerOutput, ...] = (), execution_evidence: dict[str, JsonValue] = <factory>, failure: review0_sampler_protocol.SamplerFailure | None = None, inapplicable: review0_sampler_protocol.SamplerInapplicable | None = None) -> None\""
  },
  "distribution": "review0-sampler-protocol",
  "module": "review0_sampler_protocol",
  "name": "SamplerResultPayload",
  "unit": "export"
}
```

</details>
