# stove0_review_sampler_protocol.SamplerRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-protocol:stove0-review-sampler-protocol-samplerrequest:8b595fc3e7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-98a9b591fa"></a>
- <a id="s-c897f06c0a"></a>`distribution`: `stove0-review-sampler-protocol`
- <a id="s-de46c5a853"></a>`module`: `stove0_review_sampler_protocol`
- <a id="s-af698aeef5"></a>`name`: `SamplerRequest`
- <a id="s-002225b5b8"></a>`unit`: `export`

### Declared structure

- <a id="s-883b445e4b"></a>`kind`: `"class"`
- <a id="s-264f11c78c"></a>`signature`: `"\"(*, format: Literal['stove0-review-sampler-request/v1'] = 'stove0-review-sampler-request/v1', sampler_descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], workspace_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], inputs: Annotated[tuple[stove0_review_sampler_protocol.SamplerInput, ...], MinLen(min_length=1)], windows: Annotated[tuple[stove0_review_sampler_protocol.SamplerWindow, ...], MinLen(min_length=1)], portable_intent: dict[str, JsonValue], maximum_output_bytes: Annotated[int, Ge(ge=1), Le(le=1099511627776)], timeout_seconds: Annotated[int, Ge(ge=1), Le(le=86400)], cancellation_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], request_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""`

#### Validated model schema

<a id="s-e958c04ed7"></a>
- <a id="s-449f439d46"></a>`title`: SamplerRequest
- <a id="s-031b8e7636"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-95ef59c592"></a>`cancellation_path` | yes | type="string"; minLength=1; maxLength=4096 |  |
| <a id="s-0ee1b5ec51"></a>`format` | no | type="string"; const="stove0-review-sampler-request/v1" |  |
| <a id="s-12f1a70259"></a>`inputs` | yes | type="array"; minItems=1; items=(#/$defs/SamplerInput) |  |
| <a id="s-3faf0eddb5"></a>`maximum_output_bytes` | yes | type="integer"; minimum=1; maximum=1099511627776 |  |
| <a id="s-2f30679eba"></a>`portable_intent` | yes | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-c05fa076b8"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4173cedc4b"></a>`sampler_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9d2fa7cdf8"></a>`timeout_seconds` | yes | type="integer"; minimum=1; maximum=86400 |  |
| <a id="s-8f679e0056"></a>`windows` | yes | type="array"; minItems=1; items=(#/$defs/SamplerWindow) |  |
| <a id="s-f46379ed20"></a>`workspace_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-9ab47fe995"></a>`JsonValue` | empty object |
| <a id="s-70bfab0b91"></a>`SamplerInput` | type="object"; fields=`bytes`, `id`, `media_type`, `path`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-f9ad7a1f9a"></a>`SamplerWindow` | type="object"; fields=`duration_ms`, `id`, `input_id`, `output_path`, `start_ms`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [stove0_review_sampler_protocol.SamplerRequest.verify_digest](stove0-review-sampler-protocol-samplerrequest-verify-digest.md)
- [stove0_review_sampler_protocol.SamplerRequest.seal](stove0-review-sampler-protocol-samplerrequest-seal.md)

## Governing policies

- <a id="pa-b5ff25d95a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../../../evidence/sources.md#src-25c43eb779) — `reference/stove0/targets/review/sampler/protocol/src/stove0_review_sampler_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_protocol.SamplerRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 90ceb5dfee66f59f4177f42a4264caf28606cd5df389cc6460d265771a61710d -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
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
    },
    "signature": "\"(*, format: Literal['stove0-review-sampler-request/v1'] = 'stove0-review-sampler-request/v1', sampler_descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], workspace_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], inputs: Annotated[tuple[stove0_review_sampler_protocol.SamplerInput, ...], MinLen(min_length=1)], windows: Annotated[tuple[stove0_review_sampler_protocol.SamplerWindow, ...], MinLen(min_length=1)], portable_intent: dict[str, JsonValue], maximum_output_bytes: Annotated[int, Ge(ge=1), Le(le=1099511627776)], timeout_seconds: Annotated[int, Ge(ge=1), Le(le=86400)], cancellation_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], request_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "stove0-review-sampler-protocol",
  "module": "stove0_review_sampler_protocol",
  "name": "SamplerRequest",
  "unit": "export"
}
```
