# stove0_review_sampler_support.SamplerConformanceResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-support:stove0-review-sampler-support-samplerconf-15c2d6a093:ee479321c1 -->

Exact externally visible contract owned by this semantic dossier.

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
- <a id="s-c76ee58308"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1207eb1ddb"></a>`coverage` | yes | #/$defs/SamplerConformanceCoverage |  |
| <a id="s-98efa3f750"></a>`format` | no | type="string"; const="stove0-review-sampler-conformance-result/v1" |  |
| <a id="s-1fa60bf9ca"></a>`request` | no | anyOf=#/$defs/SamplerRequest \| type="null" |  |
| <a id="s-f2c339cbf1"></a>`sample` | no | anyOf=#/$defs/SamplerResult \| type="null" |  |
| <a id="s-ae753ed1e9"></a>`sampler` | yes | #/$defs/SamplerDescriptor |  |
| <a id="s-a70194c823"></a>`sampling` | yes | type="string"; enum=["exercised","not-exercised"] |  |
| <a id="s-cac2e9cfe1"></a>`status` | yes | type="string"; enum=["conformant","inspected"] |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-091bbcfc81"></a>`JsonSchemaDocument` | type="object"; fields=`dialect`, `format_policy`, `id`, `schema`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-c6314db76b"></a>`JsonValue` | empty object |
| <a id="s-2f33eeab5d"></a>`SamplerConformanceCoverage` | type="object"; fields=`advertised`, `complete`, `exercised`; additional keys=`additionalProperties`, `required` |
| <a id="s-e0181b32dd"></a>`SamplerDescriptor` | type="object"; fields=`descriptor_sha256`, `image_digest`, `implementation_id`, `implementation_version`, `output_role`, `portable_intent_schema`, `primary_operation_contract_sha256`, `primary_operation_id`, `protocol`, `source_revision`; additional keys=`additionalProperties`, `required` |
| <a id="s-370160b285"></a>`SamplerFailure` | type="object"; fields=`code`, `message`, `retryable`; additional keys=`additionalProperties`, `required` |
| <a id="s-17ca998e9b"></a>`SamplerInapplicable` | type="object"; fields=`code`, `message`; additional keys=`additionalProperties`, `required` |
| <a id="s-1490844faa"></a>`SamplerInput` | type="object"; fields=`bytes`, `id`, `media_type`, `path`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-ec9a5ec3e6"></a>`SamplerOutput` | type="object"; fields=`bytes`, `derived_from`, `id`, `media_type`, `path`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-12ed1ee149"></a>`SamplerRequest` | type="object"; fields=`cancellation_path`, `format`, `inputs`, `maximum_output_bytes`, `portable_intent`, `request_sha256`, `sampler_descriptor_sha256`, `timeout_seconds`, `windows`, `workspace_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-03ceac90a8"></a>`SamplerResult` | type="object"; fields=`execution_evidence`, `failure`, `format`, `inapplicable`, `outputs`, `request_sha256`, `result_sha256`, `sampler_descriptor_sha256`, `state`; additional keys=`additionalProperties`, `required` |
| <a id="s-1ad9f95792"></a>`SamplerWindow` | type="object"; fields=`duration_ms`, `id`, `input_id`, `output_path`, `start_ms`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [stove0_review_sampler_support.SamplerConformanceResult.validate_result](stove0-review-sampler-support-samplerconformanceresult-validate-result.md)

## Governing policies

- <a id="pa-819d258632"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-support:stove0_review_sampler_support](../../../evidence/sources.md#src-6dd798b0df) — `reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_support.SamplerConformanceResult`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c8c76f15bbdf9eb3f6b1709d7506ece6c1d6b134f04833ac43c744d8483130e4 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "JsonSchemaDocument": {
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
            "schema": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "id",
            "sha256",
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
              "$ref": "#/$defs/JsonSchemaDocument"
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
