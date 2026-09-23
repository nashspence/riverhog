# review0_sampler_lib.SamplerConformanceResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-lib:review0-sampler-lib-samplerconformanceresult:9ef90537e9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-eed625656f"></a>
- <a id="s-929801060b"></a>`distribution`: `review0-sampler-lib`
- <a id="s-c07c2b4a5e"></a>`module`: `review0_sampler_lib`
- <a id="s-c201129da3"></a>`name`: `SamplerConformanceResult`
- <a id="s-19b570f010"></a>`unit`: `export`

### Declared structure

- <a id="s-eeca418b8e"></a>`kind`: `"class"`
- <a id="s-172a498197"></a>`signature`: `"\"(*, format: Literal['review0-sampler-conformance-result/v1'] = 'review0-sampler-conformance-result/v1', status: Literal['conformant', 'inspected'], sampler: review0_sampler_protocol.SamplerDescriptor, coverage: review0_sampler_lib.conformance.SamplerConformanceCoverage, sampling: Literal['exercised', 'not-exercised'], request: review0_sampler_protocol.SamplerRequest \| None = None, sample: review0_sampler_protocol.SamplerResult \| None = None) -> None\""`

#### Validated model schema

<a id="s-a8b24e4594"></a>

- <a id="s-a00e6c86df"></a>`type`: `"object"`
- <a id="s-733c257b44"></a>`additionalProperties`: `false`
- <a id="s-cc1dbad0bf"></a>`required`: `["status","sampler","coverage","sampling"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3fb17dbcaa"></a>`coverage` | yes | [SamplerConformanceCoverage](#s-3578e6a0e3) |  |
| <a id="s-02190a1807"></a>`format` | no | type="string"; const="review0-sampler-conformance-result/v1"; default="review0-sampler-conformance-result/v1" |  |
| <a id="s-a26325364f"></a>`request` | no | anyOf=[([SamplerRequest](#s-e0db44d992)); (type="null")]; default=null |  |
| <a id="s-4a2a936995"></a>`sample` | no | anyOf=[([SamplerResult](#s-b341c79a3c)); (type="null")]; default=null |  |
| <a id="s-c2d6f957cf"></a>`sampler` | yes | [SamplerDescriptor](#s-82319b722a) |  |
| <a id="s-16e1a1aba0"></a>`sampling` | yes | type="string"; enum=["exercised","not-exercised"] |  |
| <a id="s-69c9aae6a3"></a>`status` | yes | type="string"; enum=["conformant","inspected"] |  |

##### Definitions

- [JsonSchemaValidationProfile](#s-af2d9e29ea)
- [JsonValue](#s-536bab824b)
- [SamplerConformanceCoverage](#s-3578e6a0e3)
- [SamplerDescriptor](#s-82319b722a)
- [SamplerFailure](#s-3c9fd84feb)
- [SamplerInapplicable](#s-2bccc26a67)
- [SamplerInput](#s-ba0d3196ee)
- [SamplerOutput](#s-cca130b404)
- [SamplerRequest](#s-e0db44d992)
- [SamplerResult](#s-b341c79a3c)
- [SamplerWindow](#s-c8b3ebf5c0)

##### <a id="s-af2d9e29ea"></a>definition `JsonSchemaValidationProfile`

- <a id="s-c1c8d30eac"></a>`type`: `"object"`
- <a id="s-2e2e299097"></a>`additionalProperties`: `false`
- <a id="s-f64d1997e6"></a>`required`: `["id","profile_sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1beaa5227f"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-255e04f66e"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-7de2a5ec7f"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-34cd0564dd"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0e51d9530f"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-536bab824b)) |  |

##### <a id="s-536bab824b"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-3578e6a0e3"></a>definition `SamplerConformanceCoverage`

- <a id="s-88ea37d03c"></a>`type`: `"object"`
- <a id="s-8d031d75db"></a>`additionalProperties`: `false`
- <a id="s-600d7f101e"></a>`required`: `["exercised","complete"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7e123d1f94"></a>`advertised` | no | type="integer"; const=1; default=1 |  |
| <a id="s-465db6a6a8"></a>`complete` | yes | type="boolean" |  |
| <a id="s-a0f94b1ef9"></a>`exercised` | yes | type="integer"; minimum=0; maximum=1 |  |

##### <a id="s-82319b722a"></a>definition `SamplerDescriptor`

- <a id="s-d822262019"></a>`type`: `"object"`
- <a id="s-c012048db3"></a>`additionalProperties`: `false`
- <a id="s-5566cece52"></a>`required`: `["implementation_id","implementation_version","source_revision","image_digest","primary_operation_id","primary_operation_contract_sha256","portable_intent_schema","output_role","descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9feb862c3f"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2089be8bce"></a>`image_digest` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-43fab9dd69"></a>`implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-491387d776"></a>`implementation_version` | yes | type="string"; maxLength=120; minLength=1 |  |
| <a id="s-cb27355950"></a>`output_role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d8a8e84750"></a>`portable_intent_schema` | yes | [JsonSchemaValidationProfile](#s-af2d9e29ea) |  |
| <a id="s-3b3606c473"></a>`primary_operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d5cc70436f"></a>`primary_operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d7d3e4b535"></a>`protocol` | no | type="string"; const="review0-sampler/v1"; default="review0-sampler/v1" |  |
| <a id="s-98323331a5"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |

##### <a id="s-3c9fd84feb"></a>definition `SamplerFailure`

- <a id="s-0b3f766bf3"></a>`type`: `"object"`
- <a id="s-5be2d24ba1"></a>`additionalProperties`: `false`
- <a id="s-3917acab2b"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3bf71a9281"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-dfc25db77a"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-a654cd690e"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-2bccc26a67"></a>definition `SamplerInapplicable`

- <a id="s-56012d4169"></a>`type`: `"object"`
- <a id="s-4d4769d647"></a>`additionalProperties`: `false`
- <a id="s-46bdcb42ae"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-98fba1a6fd"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-1d32e956ed"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-ba0d3196ee"></a>definition `SamplerInput`

- <a id="s-3ef405b33a"></a>`type`: `"object"`
- <a id="s-0836f3a66c"></a>`additionalProperties`: `false`
- <a id="s-4d18082e89"></a>`required`: `["id","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-34e0e65154"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-bb315ac6b3"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-8ec961b0ad"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-4dd388fa6f"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-3eae35c36a"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-cca130b404"></a>definition `SamplerOutput`

- <a id="s-1120e4f147"></a>`type`: `"object"`
- <a id="s-f9058e02c2"></a>`additionalProperties`: `false`
- <a id="s-47072a586c"></a>`required`: `["id","path","bytes","sha256","media_type","derived_from"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ff00dd24e1"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-fb151256cf"></a>`derived_from` | yes | type="array"; items=(type="string"); minItems=1 |  |
| <a id="s-ed82362b8b"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-efa1ba1309"></a>`media_type` | yes | type="string"; maxLength=255; minLength=1 |  |
| <a id="s-7e13ad71e4"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-56cfa8d782"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-e0db44d992"></a>definition `SamplerRequest`

- <a id="s-7b6b296e52"></a>`type`: `"object"`
- <a id="s-101d754711"></a>`additionalProperties`: `false`
- <a id="s-1f559c3f36"></a>`required`: `["sampler_descriptor_sha256","workspace_id","inputs","windows","portable_intent","maximum_output_bytes","timeout_seconds","cancellation_path","request_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a4b223823f"></a>`cancellation_path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-fb70f226ee"></a>`format` | no | type="string"; const="review0-sampler-request/v1"; default="review0-sampler-request/v1" |  |
| <a id="s-5b01794546"></a>`inputs` | yes | type="array"; items=([SamplerInput](#s-ba0d3196ee)); minItems=1 |  |
| <a id="s-a90d21e460"></a>`maximum_output_bytes` | yes | type="integer"; minimum=1; maximum=1099511627776 |  |
| <a id="s-fde6dd76f3"></a>`portable_intent` | yes | type="object"; additionalProperties=([JsonValue](#s-536bab824b)) |  |
| <a id="s-b518dd98e8"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0d011eedfe"></a>`sampler_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f9593aebc0"></a>`timeout_seconds` | yes | type="integer"; minimum=1; maximum=86400 |  |
| <a id="s-7d317888f5"></a>`windows` | yes | type="array"; items=([SamplerWindow](#s-c8b3ebf5c0)); minItems=1 |  |
| <a id="s-02679fbf94"></a>`workspace_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-b341c79a3c"></a>definition `SamplerResult`

- <a id="s-dbef37abc1"></a>`type`: `"object"`
- <a id="s-d5d441e236"></a>`additionalProperties`: `false`
- <a id="s-7466cce1cc"></a>`required`: `["request_sha256","sampler_descriptor_sha256","state","result_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ba0744c60b"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-536bab824b)) |  |
| <a id="s-98c4758aac"></a>`failure` | no | anyOf=[([SamplerFailure](#s-3c9fd84feb)); (type="null")]; default=null |  |
| <a id="s-3c0c2c6041"></a>`format` | no | type="string"; const="review0-sampler-result/v1"; default="review0-sampler-result/v1" |  |
| <a id="s-dbdc052843"></a>`inapplicable` | no | anyOf=[([SamplerInapplicable](#s-2bccc26a67)); (type="null")]; default=null |  |
| <a id="s-d8ee5d4070"></a>`outputs` | no | type="array"; default=[]; items=([SamplerOutput](#s-cca130b404)) |  |
| <a id="s-a69e0b7906"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c130c8593a"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4de5a4b5ac"></a>`sampler_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-931944f662"></a>`state` | yes | type="string"; enum=["succeeded","inapplicable","failed","canceled"] |  |

##### <a id="s-c8b3ebf5c0"></a>definition `SamplerWindow`

- <a id="s-98743e8003"></a>`type`: `"object"`
- <a id="s-c063673892"></a>`additionalProperties`: `false`
- <a id="s-04c3384f61"></a>`required`: `["id","input_id","start_ms","duration_ms","output_path"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3779177cd3"></a>`duration_ms` | yes | type="integer"; minimum=1 |  |
| <a id="s-08b6f4e850"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-c4a625a251"></a>`input_id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-6ad567a383"></a>`output_path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-4884f8cdc6"></a>`start_ms` | yes | type="integer"; minimum=0 |  |

## Maintained corroboration

### Related interface records

- [validate_result](review0-sampler-lib-samplerconformanceresult-validate-result.md)

## Governing policies

- <a id="pa-2321633824"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-lib:review0_sampler_lib](../../../evidence/sources/authorities.md#src-d1d6c03a07) — [some-implementations/stove0/review0/sampler/support/src/review0\_sampler\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/support/src/review0_sampler_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_lib.SamplerConformanceResult`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b7a627780ceb2545afb7ae4b4e441fa4c7fc1481c994939752a7dc023e769db9 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "JsonSchemaValidationProfile": {
          "additionalProperties": false,
          "properties": {
            "dialect": {
              "const": "https://json-schema.org/draft/2020-12/schema",
              "default": "https://json-schema.org/draft/2020-12/schema",
              "type": "string"
            },
            "format_policy": {
              "const": "annotation-only",
              "default": "annotation-only",
              "type": "string"
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "profile_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "schema": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            }
          },
          "required": [
            "id",
            "profile_sha256",
            "schema"
          ],
          "type": "object"
        },
        "JsonValue": {},
        "SamplerConformanceCoverage": {
          "additionalProperties": false,
          "properties": {
            "advertised": {
              "const": 1,
              "default": 1,
              "type": "integer"
            },
            "complete": {
              "type": "boolean"
            },
            "exercised": {
              "maximum": 1,
              "minimum": 0,
              "type": "integer"
            }
          },
          "required": [
            "exercised",
            "complete"
          ],
          "type": "object"
        },
        "SamplerDescriptor": {
          "additionalProperties": false,
          "properties": {
            "descriptor_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "image_digest": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "implementation_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "implementation_version": {
              "maxLength": 120,
              "minLength": 1,
              "type": "string"
            },
            "output_role": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "portable_intent_schema": {
              "$ref": "#/$defs/JsonSchemaValidationProfile"
            },
            "primary_operation_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "primary_operation_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "protocol": {
              "const": "review0-sampler/v1",
              "default": "review0-sampler/v1",
              "type": "string"
            },
            "source_revision": {
              "maxLength": 200,
              "minLength": 1,
              "type": "string"
            }
          },
          "required": [
            "implementation_id",
            "implementation_version",
            "source_revision",
            "image_digest",
            "primary_operation_id",
            "primary_operation_contract_sha256",
            "portable_intent_schema",
            "output_role",
            "descriptor_sha256"
          ],
          "type": "object"
        },
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
        },
        "SamplerRequest": {
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
        "SamplerResult": {
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
        "coverage": {
          "$ref": "#/$defs/SamplerConformanceCoverage"
        },
        "format": {
          "const": "review0-sampler-conformance-result/v1",
          "default": "review0-sampler-conformance-result/v1",
          "type": "string"
        },
        "request": {
          "anyOf": [
            {
              "$ref": "#/$defs/SamplerRequest"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "sample": {
          "anyOf": [
            {
              "$ref": "#/$defs/SamplerResult"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "sampler": {
          "$ref": "#/$defs/SamplerDescriptor"
        },
        "sampling": {
          "enum": [
            "exercised",
            "not-exercised"
          ],
          "type": "string"
        },
        "status": {
          "enum": [
            "conformant",
            "inspected"
          ],
          "type": "string"
        }
      },
      "required": [
        "status",
        "sampler",
        "coverage",
        "sampling"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['review0-sampler-conformance-result/v1'] = 'review0-sampler-conformance-result/v1', status: Literal['conformant', 'inspected'], sampler: review0_sampler_protocol.SamplerDescriptor, coverage: review0_sampler_lib.conformance.SamplerConformanceCoverage, sampling: Literal['exercised', 'not-exercised'], request: review0_sampler_protocol.SamplerRequest | None = None, sample: review0_sampler_protocol.SamplerResult | None = None) -> None\""
  },
  "distribution": "review0-sampler-lib",
  "module": "review0_sampler_lib",
  "name": "SamplerConformanceResult",
  "unit": "export"
}
```

</details>
