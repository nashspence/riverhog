# stove0_review_sampler_protocol.SamplerResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-protocol:stove0-review-sampler-protocol-samplerresult:22baf9c97e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cfd5b77ea0"></a>
- <a id="s-58cef6fb15"></a>`distribution`: `stove0-review-sampler-protocol`
- <a id="s-bea57ff4d1"></a>`module`: `stove0_review_sampler_protocol`
- <a id="s-5052e9afe7"></a>`name`: `SamplerResult`
- <a id="s-981a05f65c"></a>`unit`: `export`

### Declared structure

- <a id="s-7ed6a0cb37"></a>`kind`: `"class"`
- <a id="s-af299ea1c9"></a>`signature`: `"\"(*, format: Literal['stove0-review-sampler-result/v1'] = 'stove0-review-sampler-result/v1', request_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], sampler_descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], state: Literal['succeeded', 'inapplicable', 'failed', 'canceled'], outputs: tuple[stove0_review_sampler_protocol.SamplerOutput, ...] = (), execution_evidence: dict[str, JsonValue] = <factory>, failure: stove0_review_sampler_protocol.SamplerFailure \| None = None, inapplicable: stove0_review_sampler_protocol.SamplerInapplicable \| None = None, result_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""`

#### Validated model schema

<a id="s-ddc2d47429"></a>

- <a id="s-f39c926409"></a>`type`: `"object"`
- <a id="s-c1a04d6d97"></a>`additionalProperties`: `false`
- <a id="s-cec7352851"></a>`required`: `["request_sha256","sampler_descriptor_sha256","state","result_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-394f8f303a"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-6a3e7e56cf)) |  |
| <a id="s-b97311fe39"></a>`failure` | no | anyOf=([SamplerFailure](#s-a17aaf3980)) \| (type="null"); default=null |  |
| <a id="s-10227530d7"></a>`format` | no | type="string"; const="stove0-review-sampler-result/v1"; default="stove0-review-sampler-result/v1" |  |
| <a id="s-1b23e0cc50"></a>`inapplicable` | no | anyOf=([SamplerInapplicable](#s-6e25d47d69)) \| (type="null"); default=null |  |
| <a id="s-6f7900bd15"></a>`outputs` | no | type="array"; default=[]; items=([SamplerOutput](#s-256a9177b4)) |  |
| <a id="s-3edc1cd2b7"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-afc9400c13"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-86c3302f37"></a>`sampler_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2a7a30c5f9"></a>`state` | yes | type="string"; enum=["succeeded","inapplicable","failed","canceled"] |  |

##### Definitions

- [JsonValue](#s-6a3e7e56cf)
- [SamplerFailure](#s-a17aaf3980)
- [SamplerInapplicable](#s-6e25d47d69)
- [SamplerOutput](#s-256a9177b4)

##### <a id="s-6a3e7e56cf"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-a17aaf3980"></a>definition `SamplerFailure`

- <a id="s-aed2cca7c6"></a>`type`: `"object"`
- <a id="s-2b5f08286b"></a>`additionalProperties`: `false`
- <a id="s-e6564ba871"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bf3f02a5f3"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-db3dec340b"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-58394b932e"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-6e25d47d69"></a>definition `SamplerInapplicable`

- <a id="s-8cfc1c123a"></a>`type`: `"object"`
- <a id="s-62d863ff57"></a>`additionalProperties`: `false`
- <a id="s-48634c7917"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-31c0bac20c"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-b68ca6c57f"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-256a9177b4"></a>definition `SamplerOutput`

- <a id="s-99d38f4949"></a>`type`: `"object"`
- <a id="s-837ca6376a"></a>`additionalProperties`: `false`
- <a id="s-53d6d6e46b"></a>`required`: `["id","path","bytes","sha256","media_type","derived_from"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bdb31d4122"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-27ed5691c7"></a>`derived_from` | yes | type="array"; items=(type="string"); minItems=1 |  |
| <a id="s-b2a63b5b1d"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-505883ef08"></a>`media_type` | yes | type="string"; maxLength=255; minLength=1 |  |
| <a id="s-3d25dbbac4"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-3c4c09c0cc"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [canonical_outputs](stove0-review-sampler-protocol-samplerresult-canonical-outputs.md)
- [verify_digest](stove0-review-sampler-protocol-samplerresult-verify-digest.md)
- [state_shape](stove0-review-sampler-protocol-samplerresult-state-shape.md)
- [seal](stove0-review-sampler-protocol-samplerresult-seal.md)

## Governing policies

- <a id="pa-7194fe96cf"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../../../evidence/sources.md#src-25c43eb779) — `reference/stove0/targets/review/sampler/protocol/src/stove0_review_sampler_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_protocol.SamplerResult`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bbd0775b4a6d10b1e84933489f43e96c938a9bd4e89e05032ffd7ac1a4939827 -->

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
    "signature": "\"(*, format: Literal['stove0-review-sampler-result/v1'] = 'stove0-review-sampler-result/v1', request_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], sampler_descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], state: Literal['succeeded', 'inapplicable', 'failed', 'canceled'], outputs: tuple[stove0_review_sampler_protocol.SamplerOutput, ...] = (), execution_evidence: dict[str, JsonValue] = <factory>, failure: stove0_review_sampler_protocol.SamplerFailure | None = None, inapplicable: stove0_review_sampler_protocol.SamplerInapplicable | None = None, result_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "stove0-review-sampler-protocol",
  "module": "stove0_review_sampler_protocol",
  "name": "SamplerResult",
  "unit": "export"
}
```

</details>
