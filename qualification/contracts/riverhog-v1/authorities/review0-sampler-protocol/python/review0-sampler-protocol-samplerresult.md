# review0_sampler_protocol.SamplerResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-protocol:review0-sampler-protocol-samplerresult:65abc1915c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3563be9200"></a>
- <a id="s-b1d895b10d"></a>`distribution`: `review0-sampler-protocol`
- <a id="s-0f1446ca22"></a>`module`: `review0_sampler_protocol`
- <a id="s-fb610d05c6"></a>`name`: `SamplerResult`
- <a id="s-80d0aecea9"></a>`unit`: `export`

### Declared structure

- <a id="s-90f0fc39ea"></a>`kind`: `"class"`
- <a id="s-55e0ab73ba"></a>`signature`: `"\"(*, format: Literal['review0-sampler-result/v1'] = 'review0-sampler-result/v1', request_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], sampler_descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], state: Literal['succeeded', 'inapplicable', 'failed', 'canceled'], outputs: tuple[review0_sampler_protocol.SamplerOutput, ...] = (), execution_evidence: dict[str, JsonValue] = <factory>, failure: review0_sampler_protocol.SamplerFailure \| None = None, inapplicable: review0_sampler_protocol.SamplerInapplicable \| None = None, result_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""`

#### Validated model schema

<a id="s-0fa9a1dbae"></a>

- <a id="s-e3012f0668"></a>`type`: `"object"`
- <a id="s-1de13bae6b"></a>`additionalProperties`: `false`
- <a id="s-5fb2ddb145"></a>`required`: `["request_sha256","sampler_descriptor_sha256","state","result_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3e24f20474"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-8c0daeb701)) |  |
| <a id="s-82c4ac3189"></a>`failure` | no | anyOf=[([SamplerFailure](#s-5434273c23)); (type="null")]; default=null |  |
| <a id="s-f498bdefb5"></a>`format` | no | type="string"; const="review0-sampler-result/v1"; default="review0-sampler-result/v1" |  |
| <a id="s-cb7015cff7"></a>`inapplicable` | no | anyOf=[([SamplerInapplicable](#s-8acf73c5e8)); (type="null")]; default=null |  |
| <a id="s-0ef5e89ed7"></a>`outputs` | no | type="array"; default=[]; items=([SamplerOutput](#s-06bec3e2bb)) |  |
| <a id="s-3ddd530848"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-67d9b0415e"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0bfa67eb1c"></a>`sampler_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f774b95832"></a>`state` | yes | type="string"; enum=["succeeded","inapplicable","failed","canceled"] |  |

##### Definitions

- [JsonValue](#s-8c0daeb701)
- [SamplerFailure](#s-5434273c23)
- [SamplerInapplicable](#s-8acf73c5e8)
- [SamplerOutput](#s-06bec3e2bb)

##### <a id="s-8c0daeb701"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-5434273c23"></a>definition `SamplerFailure`

- <a id="s-a2fef09a8b"></a>`type`: `"object"`
- <a id="s-305c2f91c0"></a>`additionalProperties`: `false`
- <a id="s-b4d21ffd04"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c8aee5efa8"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-45aa6f4e87"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-88bb7430ed"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-8acf73c5e8"></a>definition `SamplerInapplicable`

- <a id="s-7b3e788721"></a>`type`: `"object"`
- <a id="s-d8bcfad555"></a>`additionalProperties`: `false`
- <a id="s-8812178f66"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8fd583be73"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-29d647e828"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-06bec3e2bb"></a>definition `SamplerOutput`

- <a id="s-828b5d9b04"></a>`type`: `"object"`
- <a id="s-fcad48d962"></a>`additionalProperties`: `false`
- <a id="s-ac74ba7711"></a>`required`: `["id","path","bytes","sha256","media_type","derived_from"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-424f9a249a"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-1070391761"></a>`derived_from` | yes | type="array"; items=(type="string"); minItems=1 |  |
| <a id="s-d4abaa3949"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-dc7c744f19"></a>`media_type` | yes | type="string"; maxLength=255; minLength=1 |  |
| <a id="s-cf603547a6"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-f2a1736193"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [canonical_outputs](review0-sampler-protocol-samplerresult-canonical-outputs.md)
- [seal](review0-sampler-protocol-samplerresult-seal.md)
- [state_shape](review0-sampler-protocol-samplerresult-state-shape.md)
- [verify_digest](review0-sampler-protocol-samplerresult-verify-digest.md)

## Governing policies

- <a id="pa-d6904bb101"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-protocol:review0_sampler_protocol](../../../evidence/sources/authorities.md#src-317a05ab5e) — [some-implementations/stove0/review0/sampler/protocol/src/review0\_sampler\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/protocol/src/review0_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_protocol.SamplerResult`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a4ed69445efe1685247fa48dad0f1f5bad6e4c09087db968874e939115ce5a1b -->

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
        "result_sha256": {
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
        "state",
        "result_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['review0-sampler-result/v1'] = 'review0-sampler-result/v1', request_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], sampler_descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], state: Literal['succeeded', 'inapplicable', 'failed', 'canceled'], outputs: tuple[review0_sampler_protocol.SamplerOutput, ...] = (), execution_evidence: dict[str, JsonValue] = <factory>, failure: review0_sampler_protocol.SamplerFailure | None = None, inapplicable: review0_sampler_protocol.SamplerInapplicable | None = None, result_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "review0-sampler-protocol",
  "module": "review0_sampler_protocol",
  "name": "SamplerResult",
  "unit": "export"
}
```

</details>
