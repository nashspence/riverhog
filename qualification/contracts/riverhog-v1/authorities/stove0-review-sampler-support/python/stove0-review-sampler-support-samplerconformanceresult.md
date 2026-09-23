# stove0_review_sampler_support.SamplerConformanceResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-support:stove0-review-sampler-support-samplerconf-15c2d6a093:ee479321c1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9db013f7de"></a>
- <a id="s-44a0d7e79b"></a>`distribution`: `stove0-review-sampler-support`
- <a id="s-64c3edbcd1"></a>`module`: `stove0_review_sampler_support`
- <a id="s-468fbe5cc1"></a>`name`: `SamplerConformanceResult`
- <a id="s-6867d7b6b8"></a>`unit`: `export`

### Declared structure

- <a id="s-2da231a999"></a>`kind`: `"class"`
- <a id="s-adc798fd4e"></a>`signature`: `"\"(*, format: Literal['stove0-review-sampler-conformance-result/v1'] = 'stove0-review-sampler-conformance-result/v1', status: Literal['conformant', 'inspected'], sampler: stove0_review_sampler_protocol.SamplerDescriptor, coverage: stove0_review_sampler_support.conformance.SamplerConformanceCoverage, sampling: Literal['exercised', 'not-exercised'], request: stove0_review_sampler_protocol.SamplerRequest \| None = None, sample: stove0_review_sampler_protocol.SamplerResult \| None = None) -> None\""`

#### Validated model schema

<a id="s-ef0a3cc4f2"></a>

- <a id="s-c76ee58308"></a>`type`: `"object"`
- <a id="s-b8ef7f5c09"></a>`additionalProperties`: `false`
- <a id="s-f4262376c0"></a>`required`: `["status","sampler","coverage","sampling"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1207eb1ddb"></a>`coverage` | yes | [SamplerConformanceCoverage](#s-2f33eeab5d) |  |
| <a id="s-98efa3f750"></a>`format` | no | type="string"; const="stove0-review-sampler-conformance-result/v1"; default="stove0-review-sampler-conformance-result/v1" |  |
| <a id="s-1fa60bf9ca"></a>`request` | no | anyOf=[([SamplerRequest](#s-12ed1ee149)); (type="null")]; default=null |  |
| <a id="s-f2c339cbf1"></a>`sample` | no | anyOf=[([SamplerResult](#s-03ceac90a8)); (type="null")]; default=null |  |
| <a id="s-ae753ed1e9"></a>`sampler` | yes | [SamplerDescriptor](#s-e0181b32dd) |  |
| <a id="s-a70194c823"></a>`sampling` | yes | type="string"; enum=["exercised","not-exercised"] |  |
| <a id="s-cac2e9cfe1"></a>`status` | yes | type="string"; enum=["conformant","inspected"] |  |

##### Definitions

- [JsonSchemaValidationProfile](#s-a3f785a376)
- [JsonValue](#s-c6314db76b)
- [SamplerConformanceCoverage](#s-2f33eeab5d)
- [SamplerDescriptor](#s-e0181b32dd)
- [SamplerFailure](#s-370160b285)
- [SamplerInapplicable](#s-17ca998e9b)
- [SamplerInput](#s-1490844faa)
- [SamplerOutput](#s-ec9a5ec3e6)
- [SamplerRequest](#s-12ed1ee149)
- [SamplerResult](#s-03ceac90a8)
- [SamplerWindow](#s-1ad9f95792)

##### <a id="s-a3f785a376"></a>definition `JsonSchemaValidationProfile`

- <a id="s-9409578606"></a>`type`: `"object"`
- <a id="s-5d2fbe6a76"></a>`additionalProperties`: `false`
- <a id="s-92557b755f"></a>`required`: `["id","profile_sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-86efd8bd59"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-aec989d39a"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-2ff24c5033"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-6cc653d9d4"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1223a941cb"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-c6314db76b)) |  |

##### <a id="s-c6314db76b"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-2f33eeab5d"></a>definition `SamplerConformanceCoverage`

- <a id="s-fb856e0b7d"></a>`type`: `"object"`
- <a id="s-f637dc829f"></a>`additionalProperties`: `false`
- <a id="s-287b63e870"></a>`required`: `["exercised","complete"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-31a281b13b"></a>`advertised` | no | type="integer"; const=1; default=1 |  |
| <a id="s-47ab2a32f6"></a>`complete` | yes | type="boolean" |  |
| <a id="s-d6fa9e324e"></a>`exercised` | yes | type="integer"; minimum=0; maximum=1 |  |

##### <a id="s-e0181b32dd"></a>definition `SamplerDescriptor`

- <a id="s-dce598505d"></a>`type`: `"object"`
- <a id="s-a10281dbc3"></a>`additionalProperties`: `false`
- <a id="s-6ec3a2eb24"></a>`required`: `["implementation_id","implementation_version","source_revision","image_digest","primary_operation_id","primary_operation_contract_sha256","portable_intent_schema","output_role","descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-00fcb99ee5"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7daa720782"></a>`image_digest` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f9b640c2c5"></a>`implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-9fa0835ff5"></a>`implementation_version` | yes | type="string"; maxLength=120; minLength=1 |  |
| <a id="s-c3d4a6d077"></a>`output_role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-2ea0027dc8"></a>`portable_intent_schema` | yes | [JsonSchemaValidationProfile](#s-a3f785a376) |  |
| <a id="s-6415666489"></a>`primary_operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-eb1e0dbb8c"></a>`primary_operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-28915ab7e7"></a>`protocol` | no | type="string"; const="stove0-review-sampler/v1"; default="stove0-review-sampler/v1" |  |
| <a id="s-da45e85c5c"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |

##### <a id="s-370160b285"></a>definition `SamplerFailure`

- <a id="s-3a01a3ae63"></a>`type`: `"object"`
- <a id="s-5cd7bebb0a"></a>`additionalProperties`: `false`
- <a id="s-e26adaae40"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2645f0b9ed"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-8b17977ce7"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-cdf61309db"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-17ca998e9b"></a>definition `SamplerInapplicable`

- <a id="s-e68f007a6d"></a>`type`: `"object"`
- <a id="s-e638f9b24c"></a>`additionalProperties`: `false`
- <a id="s-3a5c4dcc94"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-920284ef28"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-41a9f046db"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-1490844faa"></a>definition `SamplerInput`

- <a id="s-d487417f72"></a>`type`: `"object"`
- <a id="s-13ca397263"></a>`additionalProperties`: `false`
- <a id="s-c840662b5f"></a>`required`: `["id","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e492f16b98"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-8c969ba197"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-09bb56d275"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-8395ec7c6f"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-494822cf2b"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-ec9a5ec3e6"></a>definition `SamplerOutput`

- <a id="s-0e73ff5167"></a>`type`: `"object"`
- <a id="s-5738e7bbd1"></a>`additionalProperties`: `false`
- <a id="s-be6ef176f0"></a>`required`: `["id","path","bytes","sha256","media_type","derived_from"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c787cd2796"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-696aa5733f"></a>`derived_from` | yes | type="array"; items=(type="string"); minItems=1 |  |
| <a id="s-39bac574fc"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-c2192b1336"></a>`media_type` | yes | type="string"; maxLength=255; minLength=1 |  |
| <a id="s-d0f9502827"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-d04c9f46b4"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-12ed1ee149"></a>definition `SamplerRequest`

- <a id="s-40e09c72bd"></a>`type`: `"object"`
- <a id="s-f1441d4a87"></a>`additionalProperties`: `false`
- <a id="s-f067084263"></a>`required`: `["sampler_descriptor_sha256","workspace_id","inputs","windows","portable_intent","maximum_output_bytes","timeout_seconds","cancellation_path","request_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6d2e06f9e0"></a>`cancellation_path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-24f1e2fd3c"></a>`format` | no | type="string"; const="stove0-review-sampler-request/v1"; default="stove0-review-sampler-request/v1" |  |
| <a id="s-d99ffae044"></a>`inputs` | yes | type="array"; items=([SamplerInput](#s-1490844faa)); minItems=1 |  |
| <a id="s-e643ef30e0"></a>`maximum_output_bytes` | yes | type="integer"; minimum=1; maximum=1099511627776 |  |
| <a id="s-9dd0885262"></a>`portable_intent` | yes | type="object"; additionalProperties=([JsonValue](#s-c6314db76b)) |  |
| <a id="s-5cfe6ec5d4"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1dc48d6b98"></a>`sampler_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-590ceaa573"></a>`timeout_seconds` | yes | type="integer"; minimum=1; maximum=86400 |  |
| <a id="s-4a9dc66ef5"></a>`windows` | yes | type="array"; items=([SamplerWindow](#s-1ad9f95792)); minItems=1 |  |
| <a id="s-19309835ee"></a>`workspace_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-03ceac90a8"></a>definition `SamplerResult`

- <a id="s-9839aca5fd"></a>`type`: `"object"`
- <a id="s-283345f348"></a>`additionalProperties`: `false`
- <a id="s-0636f09ca2"></a>`required`: `["request_sha256","sampler_descriptor_sha256","state","result_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0bbf2d4ef7"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-c6314db76b)) |  |
| <a id="s-037bce996a"></a>`failure` | no | anyOf=[([SamplerFailure](#s-370160b285)); (type="null")]; default=null |  |
| <a id="s-b6dbd38f3f"></a>`format` | no | type="string"; const="stove0-review-sampler-result/v1"; default="stove0-review-sampler-result/v1" |  |
| <a id="s-3ce4af3d83"></a>`inapplicable` | no | anyOf=[([SamplerInapplicable](#s-17ca998e9b)); (type="null")]; default=null |  |
| <a id="s-e4343c91da"></a>`outputs` | no | type="array"; default=[]; items=([SamplerOutput](#s-ec9a5ec3e6)) |  |
| <a id="s-a186c372a8"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7b86ad48a6"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4542e79d5f"></a>`sampler_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f2d3d9a2fe"></a>`state` | yes | type="string"; enum=["succeeded","inapplicable","failed","canceled"] |  |

##### <a id="s-1ad9f95792"></a>definition `SamplerWindow`

- <a id="s-06751b59e2"></a>`type`: `"object"`
- <a id="s-95528a93fb"></a>`additionalProperties`: `false`
- <a id="s-c9b2daaca4"></a>`required`: `["id","input_id","start_ms","duration_ms","output_path"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b13263bad5"></a>`duration_ms` | yes | type="integer"; minimum=1 |  |
| <a id="s-138177cb5e"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-72954d2817"></a>`input_id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-8b61c35813"></a>`output_path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-5f76355508"></a>`start_ms` | yes | type="integer"; minimum=0 |  |

## Maintained corroboration

### Related interface records

- [validate_result](stove0-review-sampler-support-samplerconformanceresult-validate-result.md)

## Governing policies

- <a id="pa-819d258632"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-sampler-support:stove0_review_sampler_support](../../../evidence/sources/authorities.md#src-6dd798b0df) — [reference/stove0/targets/review/sampler/support/src/stove0\_review\_sampler\_support/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_sampler_support.SamplerConformanceResult`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 000e95b5ac18d86ecdf5ea18d48552e1625f36acfe684e3ec672512d3fbae546 -->

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
              "const": "stove0-review-sampler/v1",
              "default": "stove0-review-sampler/v1",
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
          "const": "stove0-review-sampler-conformance-result/v1",
          "default": "stove0-review-sampler-conformance-result/v1",
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
    "signature": "\"(*, format: Literal['stove0-review-sampler-conformance-result/v1'] = 'stove0-review-sampler-conformance-result/v1', status: Literal['conformant', 'inspected'], sampler: stove0_review_sampler_protocol.SamplerDescriptor, coverage: stove0_review_sampler_support.conformance.SamplerConformanceCoverage, sampling: Literal['exercised', 'not-exercised'], request: stove0_review_sampler_protocol.SamplerRequest | None = None, sample: stove0_review_sampler_protocol.SamplerResult | None = None) -> None\""
  },
  "distribution": "stove0-review-sampler-support",
  "module": "stove0_review_sampler_support",
  "name": "SamplerConformanceResult",
  "unit": "export"
}
```

</details>
