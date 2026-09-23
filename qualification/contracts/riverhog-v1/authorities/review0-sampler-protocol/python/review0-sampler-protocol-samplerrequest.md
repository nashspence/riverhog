# review0_sampler_protocol.SamplerRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-protocol:review0-sampler-protocol-samplerrequest:a1f02a3352 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9430b6f18d"></a>
- <a id="s-8b9c3e8683"></a>`distribution`: `review0-sampler-protocol`
- <a id="s-c69c484df6"></a>`module`: `review0_sampler_protocol`
- <a id="s-367adde575"></a>`name`: `SamplerRequest`
- <a id="s-0577bdff24"></a>`unit`: `export`

### Declared structure

- <a id="s-9eb266531d"></a>`kind`: `"class"`
- <a id="s-49c524230e"></a>`signature`: `"\"(*, format: Literal['review0-sampler-request/v1'] = 'review0-sampler-request/v1', sampler_descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], workspace_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], inputs: Annotated[tuple[review0_sampler_protocol.SamplerInput, ...], MinLen(min_length=1)], windows: Annotated[tuple[review0_sampler_protocol.SamplerWindow, ...], MinLen(min_length=1)], portable_intent: dict[str, JsonValue], maximum_output_bytes: Annotated[int, Ge(ge=1), Le(le=1099511627776)], timeout_seconds: Annotated[int, Ge(ge=1), Le(le=86400)], cancellation_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], request_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""`

#### Validated model schema

<a id="s-c8df8fcbb0"></a>

- <a id="s-4977b6a706"></a>`type`: `"object"`
- <a id="s-b242902b52"></a>`additionalProperties`: `false`
- <a id="s-fbb615b86b"></a>`required`: `["sampler_descriptor_sha256","workspace_id","inputs","windows","portable_intent","maximum_output_bytes","timeout_seconds","cancellation_path","request_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0644db6d81"></a>`cancellation_path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-8a8afc8876"></a>`format` | no | type="string"; const="review0-sampler-request/v1"; default="review0-sampler-request/v1" |  |
| <a id="s-2ca5aa229a"></a>`inputs` | yes | type="array"; items=([SamplerInput](#s-0f6c8dbdc0)); minItems=1 |  |
| <a id="s-4c0cc4bd58"></a>`maximum_output_bytes` | yes | type="integer"; minimum=1; maximum=1099511627776 |  |
| <a id="s-1bfcc00056"></a>`portable_intent` | yes | type="object"; additionalProperties=([JsonValue](#s-1242bebdd4)) |  |
| <a id="s-883599dbb3"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7b9a7edbd8"></a>`sampler_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e82b3754c7"></a>`timeout_seconds` | yes | type="integer"; minimum=1; maximum=86400 |  |
| <a id="s-c99ec93ff9"></a>`windows` | yes | type="array"; items=([SamplerWindow](#s-bff90ffc91)); minItems=1 |  |
| <a id="s-650c352f67"></a>`workspace_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [JsonValue](#s-1242bebdd4)
- [SamplerInput](#s-0f6c8dbdc0)
- [SamplerWindow](#s-bff90ffc91)

##### <a id="s-1242bebdd4"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-0f6c8dbdc0"></a>definition `SamplerInput`

- <a id="s-fc92ea69ca"></a>`type`: `"object"`
- <a id="s-33cfae87ee"></a>`additionalProperties`: `false`
- <a id="s-c8acc7b261"></a>`required`: `["id","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d8f54979d9"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-a80551babf"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-72f0371e1b"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-d58c72c9fb"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-fed1625497"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-bff90ffc91"></a>definition `SamplerWindow`

- <a id="s-469efb5519"></a>`type`: `"object"`
- <a id="s-4b8d5dca33"></a>`additionalProperties`: `false`
- <a id="s-739d898245"></a>`required`: `["id","input_id","start_ms","duration_ms","output_path"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2d37855ba9"></a>`duration_ms` | yes | type="integer"; minimum=1 |  |
| <a id="s-f4262e8315"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-6755c327dd"></a>`input_id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-19c1c137b3"></a>`output_path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-78e89936b7"></a>`start_ms` | yes | type="integer"; minimum=0 |  |

## Maintained corroboration

### Related interface records

- [canonical_windows](review0-sampler-protocol-samplerrequest-canonical-windows.md)
- [canonical_cancellation_path](review0-sampler-protocol-samplerrequest-canonical-cancellation-path.md)
- [canonical_inputs](review0-sampler-protocol-samplerrequest-canonical-inputs.md)
- [references_exact_inputs](review0-sampler-protocol-samplerrequest-references-exact-inputs.md)
- [seal](review0-sampler-protocol-samplerrequest-seal.md)
- [verify_digest](review0-sampler-protocol-samplerrequest-verify-digest.md)

## Governing policies

- <a id="pa-bed3437a3a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-protocol:review0_sampler_protocol](../../../evidence/sources/authorities.md#src-317a05ab5e) — [some-implementations/stove0/review0/sampler/protocol/src/review0\_sampler\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/protocol/src/review0_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_protocol.SamplerRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4f48a0adca226eeff770a554c058dc9fa6270b736aa3cfbd19d3ca8c7605ff50 -->

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
              "type": "integer"
            },
            "id": {
              "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
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
              "default": null
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
            "sha256"
          ],
          "type": "object"
        },
        "SamplerWindow": {
          "additionalProperties": false,
          "properties": {
            "duration_ms": {
              "minimum": 1,
              "type": "integer"
            },
            "id": {
              "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
              "type": "string"
            },
            "input_id": {
              "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
              "type": "string"
            },
            "output_path": {
              "maxLength": 4096,
              "minLength": 1,
              "type": "string"
            },
            "start_ms": {
              "minimum": 0,
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
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "cancellation_path": {
          "maxLength": 4096,
          "minLength": 1,
          "type": "string"
        },
        "format": {
          "const": "review0-sampler-request/v1",
          "default": "review0-sampler-request/v1",
          "type": "string"
        },
        "inputs": {
          "items": {
            "$ref": "#/$defs/SamplerInput"
          },
          "minItems": 1,
          "type": "array"
        },
        "maximum_output_bytes": {
          "maximum": 1099511627776,
          "minimum": 1,
          "type": "integer"
        },
        "portable_intent": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "type": "object"
        },
        "request_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "sampler_descriptor_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "timeout_seconds": {
          "maximum": 86400,
          "minimum": 1,
          "type": "integer"
        },
        "windows": {
          "items": {
            "$ref": "#/$defs/SamplerWindow"
          },
          "minItems": 1,
          "type": "array"
        },
        "workspace_id": {
          "pattern": "^[0-9a-f]{64}$",
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
      "type": "object"
    },
    "signature": "\"(*, format: Literal['review0-sampler-request/v1'] = 'review0-sampler-request/v1', sampler_descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], workspace_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], inputs: Annotated[tuple[review0_sampler_protocol.SamplerInput, ...], MinLen(min_length=1)], windows: Annotated[tuple[review0_sampler_protocol.SamplerWindow, ...], MinLen(min_length=1)], portable_intent: dict[str, JsonValue], maximum_output_bytes: Annotated[int, Ge(ge=1), Le(le=1099511627776)], timeout_seconds: Annotated[int, Ge(ge=1), Le(le=86400)], cancellation_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], request_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "review0-sampler-protocol",
  "module": "review0_sampler_protocol",
  "name": "SamplerRequest",
  "unit": "export"
}
```

</details>
