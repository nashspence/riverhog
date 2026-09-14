# stove0_review_sampler_protocol.SamplerResultPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-protocol:stove0-review-sampler-protocol-samplerresultpayload:a978d5723e -->

Exact externally visible contract owned by this semantic dossier.

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
- <a id="s-654625a86f"></a>`title`: SamplerResultPayload
- <a id="s-d610b6e916"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c19d683b2d"></a>`execution_evidence` | no | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-0d2aa44490"></a>`failure` | no | anyOf=#/$defs/SamplerFailure \| type="null" |  |
| <a id="s-20ed96c908"></a>`format` | no | type="string"; const="stove0-review-sampler-result/v1" |  |
| <a id="s-5f40521f3b"></a>`inapplicable` | no | anyOf=#/$defs/SamplerInapplicable \| type="null" |  |
| <a id="s-5bfb3df8b3"></a>`outputs` | no | type="array"; items=(#/$defs/SamplerOutput) |  |
| <a id="s-7a462fb9ba"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-fc7faa8e8f"></a>`sampler_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-76a98465b3"></a>`state` | yes | type="string"; enum=["succeeded","inapplicable","failed","canceled"] |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-79443c3733"></a>`JsonValue` | empty object |
| <a id="s-82b59a94c6"></a>`SamplerFailure` | type="object"; fields=`code`, `message`, `retryable`; additional keys=`additionalProperties`, `required` |
| <a id="s-8e2a7dcc74"></a>`SamplerInapplicable` | type="object"; fields=`code`, `message`; additional keys=`additionalProperties`, `required` |
| <a id="s-2c0690b424"></a>`SamplerOutput` | type="object"; fields=`bytes`, `derived_from`, `id`, `media_type`, `path`, `sha256`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [stove0_review_sampler_protocol.SamplerResultPayload.state_shape](stove0-review-sampler-protocol-samplerresultpayload-state-shape.md)
- [stove0_review_sampler_protocol.SamplerResultPayload.canonical_outputs](stove0-review-sampler-protocol-samplerresultpayload-canonical-outputs.md)

## Governing policies

- <a id="pa-e3228be7cc"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../../../evidence/sources.md#src-25c43eb779) — `reference/stove0/targets/review/sampler/protocol/src/stove0_review_sampler_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_protocol.SamplerResultPayload`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f19e83b571eab7c4dac6d8e293b7abb46edc799bce49ba87eb523c16f7aabcee -->

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
        "state"
      ],
      "title": "SamplerResultPayload",
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
