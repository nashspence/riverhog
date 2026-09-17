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

- <a id="s-031b8e7636"></a>`type`: `"object"`
- <a id="s-2b23a0a5dd"></a>`additionalProperties`: `false`
- <a id="s-96fb3e921c"></a>`required`: `["sampler_descriptor_sha256","workspace_id","inputs","windows","portable_intent","maximum_output_bytes","timeout_seconds","cancellation_path","request_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-95ef59c592"></a>`cancellation_path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-0ee1b5ec51"></a>`format` | no | type="string"; const="stove0-review-sampler-request/v1"; default="stove0-review-sampler-request/v1" |  |
| <a id="s-12f1a70259"></a>`inputs` | yes | type="array"; items=([SamplerInput](#s-70bfab0b91)); minItems=1 |  |
| <a id="s-3faf0eddb5"></a>`maximum_output_bytes` | yes | type="integer"; minimum=1; maximum=1099511627776 |  |
| <a id="s-2f30679eba"></a>`portable_intent` | yes | type="object"; additionalProperties=([JsonValue](#s-9ab47fe995)) |  |
| <a id="s-c05fa076b8"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4173cedc4b"></a>`sampler_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9d2fa7cdf8"></a>`timeout_seconds` | yes | type="integer"; minimum=1; maximum=86400 |  |
| <a id="s-8f679e0056"></a>`windows` | yes | type="array"; items=([SamplerWindow](#s-f9ad7a1f9a)); minItems=1 |  |
| <a id="s-f46379ed20"></a>`workspace_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [JsonValue](#s-9ab47fe995)
- [SamplerInput](#s-70bfab0b91)
- [SamplerWindow](#s-f9ad7a1f9a)

##### <a id="s-9ab47fe995"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-70bfab0b91"></a>definition `SamplerInput`

- <a id="s-63f45ce449"></a>`type`: `"object"`
- <a id="s-813ff751fe"></a>`additionalProperties`: `false`
- <a id="s-2bb4b9b3e0"></a>`required`: `["id","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b415320dc7"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-4a57ca9b6e"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-6bbfc32518"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-25c2f514cc"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-5cc2e2190e"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-f9ad7a1f9a"></a>definition `SamplerWindow`

- <a id="s-909bdcaabd"></a>`type`: `"object"`
- <a id="s-6a5e4fafa4"></a>`additionalProperties`: `false`
- <a id="s-31468b18a6"></a>`required`: `["id","input_id","start_ms","duration_ms","output_path"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f0be2913a9"></a>`duration_ms` | yes | type="integer"; minimum=1 |  |
| <a id="s-c29c0641cd"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-c18a951dd6"></a>`input_id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-845d3c13db"></a>`output_path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-dc3a577c1c"></a>`start_ms` | yes | type="integer"; minimum=0 |  |

## Maintained corroboration

### Related interface records

- [canonical_cancellation_path](stove0-review-sampler-protocol-samplerrequest-canonical-cancellation-path.md)
- [verify_digest](stove0-review-sampler-protocol-samplerrequest-verify-digest.md)
- [references_exact_inputs](stove0-review-sampler-protocol-samplerrequest-references-exact-inputs.md)
- [canonical_inputs](stove0-review-sampler-protocol-samplerrequest-canonical-inputs.md)
- [canonical_windows](stove0-review-sampler-protocol-samplerrequest-canonical-windows.md)
- [seal](stove0-review-sampler-protocol-samplerrequest-seal.md)

## Governing policies

- <a id="pa-b5ff25d95a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../../../evidence/sources.md#src-25c43eb779) — [reference/stove0/targets/review/sampler/protocol/src/stove0\_review\_sampler\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/sampler/protocol/src/stove0_review_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_sampler_protocol.SamplerRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 758eb57d97340ca4a9f5436b771405d808c414d2e5988527679d26cb05b1cf95 -->

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
    "signature": "\"(*, format: Literal['stove0-review-sampler-request/v1'] = 'stove0-review-sampler-request/v1', sampler_descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], workspace_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], inputs: Annotated[tuple[stove0_review_sampler_protocol.SamplerInput, ...], MinLen(min_length=1)], windows: Annotated[tuple[stove0_review_sampler_protocol.SamplerWindow, ...], MinLen(min_length=1)], portable_intent: dict[str, JsonValue], maximum_output_bytes: Annotated[int, Ge(ge=1), Le(le=1099511627776)], timeout_seconds: Annotated[int, Ge(ge=1), Le(le=86400)], cancellation_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], request_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "stove0-review-sampler-protocol",
  "module": "stove0_review_sampler_protocol",
  "name": "SamplerRequest",
  "unit": "export"
}
```

</details>
