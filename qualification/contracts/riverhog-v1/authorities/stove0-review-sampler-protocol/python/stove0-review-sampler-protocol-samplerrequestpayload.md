# stove0_review_sampler_protocol.SamplerRequestPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-protocol:stove0-review-sampler-protocol-samplerrequestpayload:a0a1dd4a83 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0352e93d68"></a>
- <a id="s-30ef560e69"></a>`distribution`: `stove0-review-sampler-protocol`
- <a id="s-a6ef43ebde"></a>`module`: `stove0_review_sampler_protocol`
- <a id="s-55f65755cf"></a>`name`: `SamplerRequestPayload`
- <a id="s-4dab40d57d"></a>`unit`: `export`

### Declared structure

- <a id="s-59ea8e069a"></a>`kind`: `"class"`
- <a id="s-90237354bf"></a>`signature`: `"\"(*, format: Literal['stove0-review-sampler-request/v1'] = 'stove0-review-sampler-request/v1', sampler_descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], workspace_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], inputs: Annotated[tuple[stove0_review_sampler_protocol.SamplerInput, ...], MinLen(min_length=1)], windows: Annotated[tuple[stove0_review_sampler_protocol.SamplerWindow, ...], MinLen(min_length=1)], portable_intent: dict[str, JsonValue], maximum_output_bytes: Annotated[int, Ge(ge=1), Le(le=1099511627776)], timeout_seconds: Annotated[int, Ge(ge=1), Le(le=86400)], cancellation_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)]) -> None\""`

#### Validated model schema

<a id="s-302a88143a"></a>

- <a id="s-8f6c694853"></a>`type`: `"object"`
- <a id="s-0472f3cbc7"></a>`additionalProperties`: `false`
- <a id="s-5cc23806a3"></a>`required`: `["sampler_descriptor_sha256","workspace_id","inputs","windows","portable_intent","maximum_output_bytes","timeout_seconds","cancellation_path"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1e14d51675"></a>`cancellation_path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-67e22a6b8e"></a>`format` | no | type="string"; const="stove0-review-sampler-request/v1"; default="stove0-review-sampler-request/v1" |  |
| <a id="s-b01b8d7f9f"></a>`inputs` | yes | type="array"; items=([SamplerInput](#s-1b43c3a26a)); minItems=1 |  |
| <a id="s-dc6349d1e9"></a>`maximum_output_bytes` | yes | type="integer"; minimum=1; maximum=1099511627776 |  |
| <a id="s-10cbe86ef8"></a>`portable_intent` | yes | type="object"; additionalProperties=([JsonValue](#s-7f2975c9a8)) |  |
| <a id="s-c3026b6ce3"></a>`sampler_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8492466ba8"></a>`timeout_seconds` | yes | type="integer"; minimum=1; maximum=86400 |  |
| <a id="s-68102daa11"></a>`windows` | yes | type="array"; items=([SamplerWindow](#s-4420d87626)); minItems=1 |  |
| <a id="s-8a7e8541ef"></a>`workspace_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [JsonValue](#s-7f2975c9a8)
- [SamplerInput](#s-1b43c3a26a)
- [SamplerWindow](#s-4420d87626)

##### <a id="s-7f2975c9a8"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-1b43c3a26a"></a>definition `SamplerInput`

- <a id="s-3a8c797899"></a>`type`: `"object"`
- <a id="s-283e07fa58"></a>`additionalProperties`: `false`
- <a id="s-05789b6831"></a>`required`: `["id","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6a8533bf55"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-65bdda7a1b"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-786f4bcbb1"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-aa8394a1b4"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-4c86473442"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-4420d87626"></a>definition `SamplerWindow`

- <a id="s-56e82d20ad"></a>`type`: `"object"`
- <a id="s-28d8bdc576"></a>`additionalProperties`: `false`
- <a id="s-28c6f5c4a7"></a>`required`: `["id","input_id","start_ms","duration_ms","output_path"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-83efaf0e72"></a>`duration_ms` | yes | type="integer"; minimum=1 |  |
| <a id="s-19a334c99f"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-0d220c4ee6"></a>`input_id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-ec8fb7cc09"></a>`output_path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-903daec653"></a>`start_ms` | yes | type="integer"; minimum=0 |  |

## Maintained corroboration

### Related interface records

- [canonical_cancellation_path](stove0-review-sampler-protocol-samplerrequestpayload-canonical-cancellation-path.md)
- [references_exact_inputs](stove0-review-sampler-protocol-samplerrequestpayload-references-exact-inputs.md)
- [canonical_inputs](stove0-review-sampler-protocol-samplerrequestpayload-canonical-inputs.md)
- [canonical_windows](stove0-review-sampler-protocol-samplerrequestpayload-canonical-windows.md)

## Governing policies

- <a id="pa-605e71c243"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../../../evidence/sources.md#src-25c43eb779) — `reference/stove0/targets/review/sampler/protocol/src/stove0_review_sampler_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_protocol.SamplerRequestPayload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0cf762f064f0f2187b75614618c617c1f184dea55fd6141fa0b2518402827ebc -->

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
          "const": "stove0-review-sampler-request/v1",
          "default": "stove0-review-sampler-request/v1",
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
    "signature": "\"(*, format: Literal['stove0-review-sampler-request/v1'] = 'stove0-review-sampler-request/v1', sampler_descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], workspace_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], inputs: Annotated[tuple[stove0_review_sampler_protocol.SamplerInput, ...], MinLen(min_length=1)], windows: Annotated[tuple[stove0_review_sampler_protocol.SamplerWindow, ...], MinLen(min_length=1)], portable_intent: dict[str, JsonValue], maximum_output_bytes: Annotated[int, Ge(ge=1), Le(le=1099511627776)], timeout_seconds: Annotated[int, Ge(ge=1), Le(le=86400)], cancellation_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)]) -> None\""
  },
  "distribution": "stove0-review-sampler-protocol",
  "module": "stove0_review_sampler_protocol",
  "name": "SamplerRequestPayload",
  "unit": "export"
}
```

</details>
