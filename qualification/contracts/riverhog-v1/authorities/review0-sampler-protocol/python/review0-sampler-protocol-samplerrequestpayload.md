# review0_sampler_protocol.SamplerRequestPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-protocol:review0-sampler-protocol-samplerrequestpayload:dd6c65ab31 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5ce93d1dc9"></a>
- <a id="s-7d7bd123fc"></a>`distribution`: `review0-sampler-protocol`
- <a id="s-e32821be2a"></a>`module`: `review0_sampler_protocol`
- <a id="s-ef4ccede24"></a>`name`: `SamplerRequestPayload`
- <a id="s-fd59a0048b"></a>`unit`: `export`

### Declared structure

- <a id="s-b084d93c47"></a>`kind`: `"class"`
- <a id="s-3fdf46ad2d"></a>`signature`: `"\"(*, format: Literal['review0-sampler-request/v1'] = 'review0-sampler-request/v1', sampler_descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], workspace_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], inputs: Annotated[tuple[review0_sampler_protocol.SamplerInput, ...], MinLen(min_length=1)], windows: Annotated[tuple[review0_sampler_protocol.SamplerWindow, ...], MinLen(min_length=1)], portable_intent: dict[str, JsonValue], maximum_output_bytes: Annotated[int, Ge(ge=1), Le(le=1099511627776)], timeout_seconds: Annotated[int, Ge(ge=1), Le(le=86400)], cancellation_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)]) -> None\""`

#### Validated model schema

<a id="s-b1cba85724"></a>

- <a id="s-b397642dee"></a>`type`: `"object"`
- <a id="s-bf8fe2ce1a"></a>`additionalProperties`: `false`
- <a id="s-2aff247a01"></a>`required`: `["sampler_descriptor_sha256","workspace_id","inputs","windows","portable_intent","maximum_output_bytes","timeout_seconds","cancellation_path"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2f84485d7b"></a>`cancellation_path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-d59c2920c4"></a>`format` | no | type="string"; const="review0-sampler-request/v1"; default="review0-sampler-request/v1" |  |
| <a id="s-7bb456c81d"></a>`inputs` | yes | type="array"; items=([SamplerInput](#s-02c4be0389)); minItems=1 |  |
| <a id="s-9e392a73f1"></a>`maximum_output_bytes` | yes | type="integer"; minimum=1; maximum=1099511627776 |  |
| <a id="s-171ca9f375"></a>`portable_intent` | yes | type="object"; additionalProperties=([JsonValue](#s-f6cb93713b)) |  |
| <a id="s-bb0fcb1b96"></a>`sampler_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ef3f91c69c"></a>`timeout_seconds` | yes | type="integer"; minimum=1; maximum=86400 |  |
| <a id="s-83658d63fc"></a>`windows` | yes | type="array"; items=([SamplerWindow](#s-190a9ed709)); minItems=1 |  |
| <a id="s-4610fa4bde"></a>`workspace_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [JsonValue](#s-f6cb93713b)
- [SamplerInput](#s-02c4be0389)
- [SamplerWindow](#s-190a9ed709)

##### <a id="s-f6cb93713b"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-02c4be0389"></a>definition `SamplerInput`

- <a id="s-856b9283cd"></a>`type`: `"object"`
- <a id="s-01ffbb33b3"></a>`additionalProperties`: `false`
- <a id="s-47ef669875"></a>`required`: `["id","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-19a528c163"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-8a389b4c0c"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-d09bf49cad"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-fd70412f5f"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-d5e60fef98"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-190a9ed709"></a>definition `SamplerWindow`

- <a id="s-f44b9f665b"></a>`type`: `"object"`
- <a id="s-a805fc0808"></a>`additionalProperties`: `false`
- <a id="s-fc8b35f479"></a>`required`: `["id","input_id","start_ms","duration_ms","output_path"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-082cad6f50"></a>`duration_ms` | yes | type="integer"; minimum=1 |  |
| <a id="s-a22f6269fc"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-21a151a3e6"></a>`input_id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-7e2b8750a0"></a>`output_path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-6ff0d062ff"></a>`start_ms` | yes | type="integer"; minimum=0 |  |

## Maintained corroboration

### Related interface records

- [references_exact_inputs](review0-sampler-protocol-samplerrequestpayload-references-exact-inputs.md)
- [canonical_cancellation_path](review0-sampler-protocol-samplerrequestpayload-canonical-cancellation-path.md)
- [canonical_inputs](review0-sampler-protocol-samplerrequestpayload-canonical-inputs.md)
- [canonical_windows](review0-sampler-protocol-samplerrequestpayload-canonical-windows.md)

## Governing policies

- <a id="pa-a88380eef7"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-protocol:review0_sampler_protocol](../../../evidence/sources/authorities.md#src-317a05ab5e) — [some-implementations/stove0/review0/sampler/protocol/src/review0\_sampler\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/protocol/src/review0_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_protocol.SamplerRequestPayload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 20ae2b4ef7baf2d852b8e229c5225b25475ee9f02b1d10f32dacd8520c1b91b6 -->

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
        "cancellation_path"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['review0-sampler-request/v1'] = 'review0-sampler-request/v1', sampler_descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], workspace_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], inputs: Annotated[tuple[review0_sampler_protocol.SamplerInput, ...], MinLen(min_length=1)], windows: Annotated[tuple[review0_sampler_protocol.SamplerWindow, ...], MinLen(min_length=1)], portable_intent: dict[str, JsonValue], maximum_output_bytes: Annotated[int, Ge(ge=1), Le(le=1099511627776)], timeout_seconds: Annotated[int, Ge(ge=1), Le(le=86400)], cancellation_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)]) -> None\""
  },
  "distribution": "review0-sampler-protocol",
  "module": "review0_sampler_protocol",
  "name": "SamplerRequestPayload",
  "unit": "export"
}
```

</details>
