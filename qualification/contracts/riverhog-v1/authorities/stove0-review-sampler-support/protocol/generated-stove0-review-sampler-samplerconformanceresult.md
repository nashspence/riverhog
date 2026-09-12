# generated:stove0-review-sampler: SamplerConformanceResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:stove0-review-sampler-support:generated-stove0-review-sampler-samplerco-d72f170b7c:470b0487a6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-support](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](index.md#f-fba3a5663ca7) |
| Contract elements | 1 |
| Extent decisions | 34 |

## External contract

<a id="s-91458a90aff6"></a>
- <a id="s-155f99f6756d"></a>`title`: SamplerConformanceResult
- <a id="s-3824ed0f76eb"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f5926c53861b"></a>`coverage` | yes | #/$defs/SamplerConformanceCoverage |  |
| <a id="s-0c55a65141eb"></a>`format` | no | type="string"; const="stove0-review-sampler-conformance-result/v1" |  |
| <a id="s-ccd482d298ea"></a>`request` | no | anyOf=#/$defs/SamplerRequest \| type="null" |  |
| <a id="s-d731a26fe4b2"></a>`sample` | no | anyOf=#/$defs/SamplerResult \| type="null" |  |
| <a id="s-27f28a3590c3"></a>`sampler` | yes | #/$defs/SamplerDescriptor |  |
| <a id="s-e308775b661a"></a>`sampling` | yes | type="string"; enum=["exercised","not-exercised"] |  |
| <a id="s-3a0ae026790d"></a>`status` | yes | type="string"; enum=["conformant","inspected"] |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-70c0e8bf4a8e"></a>`JsonSchemaDocument` | type="object"; fields=`dialect`, `format_policy`, `id`, `schema`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-859721ca5fa1"></a>`JsonValue` | empty object |
| <a id="s-94518c8e126e"></a>`SamplerConformanceCoverage` | type="object"; fields=`advertised`, `complete`, `exercised`; additional keys=`additionalProperties`, `required` |
| <a id="s-3cbd950f9ca3"></a>`SamplerDescriptor` | type="object"; fields=`descriptor_sha256`, `image_digest`, `implementation_id`, `implementation_version`, `output_role`, `portable_intent_schema`, `primary_operation_contract_sha256`, `primary_operation_id`, `protocol`, `source_revision`; additional keys=`additionalProperties`, `required` |
| <a id="s-86fc0a8ec020"></a>`SamplerFailure` | type="object"; fields=`code`, `message`, `retryable`; additional keys=`additionalProperties`, `required` |
| <a id="s-9293e5a12755"></a>`SamplerInapplicable` | type="object"; fields=`code`, `message`; additional keys=`additionalProperties`, `required` |
| <a id="s-657bf3ad58eb"></a>`SamplerInput` | type="object"; fields=`bytes`, `id`, `media_type`, `path`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-3bdca52814ce"></a>`SamplerOutput` | type="object"; fields=`bytes`, `derived_from`, `id`, `media_type`, `path`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-31a33a19b522"></a>`SamplerRequest` | type="object"; fields=`cancellation_path`, `format`, `inputs`, `maximum_output_bytes`, `portable_intent`, `request_sha256`, `sampler_descriptor_sha256`, `timeout_seconds`, `windows`, `workspace_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-36154d39411b"></a>`SamplerResult` | type="object"; fields=`execution_evidence`, `failure`, `format`, `inapplicable`, `outputs`, `request_sha256`, `result_sha256`, `sampler_descriptor_sha256`, `state`; additional keys=`additionalProperties`, `required` |
| <a id="s-0f82a2deb837"></a>`SamplerWindow` | type="object"; fields=`duration_ms`, `id`, `input_id`, `output_path`, `start_ms`; additional keys=`additionalProperties`, `required` |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-review-sampler-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-3ec7a155f956"></a>definition JsonSchemaDocument · field schema | `cardinality · entries · operational_policy` | shared above |
| <a id="s-06a13f5f97b7"></a>definition SamplerInput · field bytes | `value · schema-value · operational_policy` | shared above |
| <a id="s-6432d25c7a91"></a>definition SamplerOutput · field bytes | `value · schema-value · operational_policy` | shared above |
| <a id="s-ad304a615233"></a>definition SamplerOutput · field derived_from | `cardinality · items · operational_policy` | shared above |
| <a id="s-8e9bf2a4dcc6"></a>definition SamplerRequest · field inputs | `cardinality · items · operational_policy` | shared above |
| <a id="s-0f61fa82336e"></a>definition SamplerRequest · field portable_intent | `cardinality · entries · operational_policy` | shared above |
| <a id="s-ba92fcfa2d08"></a>definition SamplerRequest · field windows | `cardinality · items · operational_policy` | shared above |
| <a id="s-ba880d444895"></a>definition SamplerResult · field execution_evidence | `cardinality · entries · operational_policy` | shared above |
| <a id="s-8b6287177a6a"></a>definition SamplerResult · field outputs | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-bbb1edec1864"></a>definition JsonSchemaDocument · field sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-52bf1095a165"></a>definition SamplerConformanceCoverage · field exercised | `value · schema-value · contract_max` | maximum=1; minimum=0; reason="schema-maximum" |
| <a id="s-d4183e9ecd54"></a>definition SamplerDescriptor · field descriptor_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-3555292979df"></a>definition SamplerDescriptor · field image_digest | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-1dfc403081dd"></a>definition SamplerDescriptor · field implementation_version | `length · characters · contract_max` | maximum=120; minimum=1; reason="schema-maximum" |
| <a id="s-20b5af8d0442"></a>definition SamplerDescriptor · field primary_operation_contract_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-f8b6cb1c7ae5"></a>definition SamplerDescriptor · field source_revision | `length · characters · contract_max` | maximum=200; minimum=1; reason="schema-maximum" |
| <a id="s-361415775296"></a>definition SamplerFailure · field message | `length · characters · contract_max` | maximum=1000; minimum=1; reason="schema-maximum" |
| <a id="s-7450a8b3c044"></a>definition SamplerInapplicable · field message | `length · characters · contract_max` | maximum=1000; minimum=1; reason="schema-maximum" |
| <a id="s-e7e4f045ed96"></a>definition SamplerInput · field media_type · anyOf alternative 1 | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| <a id="s-37db911b0af8"></a>definition SamplerInput · field path | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| <a id="s-59e0c9921977"></a>definition SamplerInput · field sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-5e84a610c988"></a>definition SamplerOutput · field media_type | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| <a id="s-fb0c811393ba"></a>definition SamplerOutput · field path | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| <a id="s-e59534716783"></a>definition SamplerOutput · field sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-e9732894355e"></a>definition SamplerRequest · field cancellation_path | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| <a id="s-5cc372c03030"></a>definition SamplerRequest · field maximum_output_bytes | `value · schema-value · contract_max` | maximum=1099511627776; minimum=1; reason="schema-maximum" |
| <a id="s-ce4e43ab7726"></a>definition SamplerRequest · field request_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-e75aab889c30"></a>definition SamplerRequest · field sampler_descriptor_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-5f2054549486"></a>definition SamplerRequest · field timeout_seconds | `value · schema-value · contract_max` | maximum=86400; minimum=1; reason="schema-maximum" |
| <a id="s-8e4c8d92b123"></a>definition SamplerRequest · field workspace_id | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-fad6aae13286"></a>definition SamplerResult · field request_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-7e3a4328e7c6"></a>definition SamplerResult · field result_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-c913fa2c6077"></a>definition SamplerResult · field sampler_descriptor_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-97704c5295fa"></a>definition SamplerWindow · field output_path | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |

## Governing policies

- <a id="pa-602c93bbae98"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-03ea01a8c844"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)
- <a id="pa-22fb930eda16"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:stove0-review-sampler](../../../evidence/sources.md#src-b47f3f4d7b7f) — `reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/schemas.py::sampler_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-review-sampler/schemas/SamplerConformanceResult`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 04925f6f55b8bdb122d1cfdcba5aa12da892ebae3bdc0f0f0f6955364773c80f -->

```json
{
  "$defs": {
    "JsonSchemaDocument": {
      "additionalProperties": false,
      "properties": {
        "dialect": {
          "const": "https://json-schema.org/draft/2020-12/schema",
          "default": "https://json-schema.org/draft/2020-12/schema",
          "title": "Dialect",
          "type": "string"
        },
        "format_policy": {
          "const": "annotation-only",
          "default": "annotation-only",
          "title": "Format Policy",
          "type": "string"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "schema": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Schema",
          "type": "object"
        },
        "sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Sha256",
          "type": "string"
        }
      },
      "required": [
        "id",
        "sha256",
        "schema"
      ],
      "title": "JsonSchemaDocument",
      "type": "object"
    },
    "JsonValue": {},
    "SamplerConformanceCoverage": {
      "additionalProperties": false,
      "properties": {
        "advertised": {
          "const": 1,
          "default": 1,
          "title": "Advertised",
          "type": "integer"
        },
        "complete": {
          "title": "Complete",
          "type": "boolean"
        },
        "exercised": {
          "maximum": 1,
          "minimum": 0,
          "title": "Exercised",
          "type": "integer"
        }
      },
      "required": [
        "exercised",
        "complete"
      ],
      "title": "SamplerConformanceCoverage",
      "type": "object"
    },
    "SamplerDescriptor": {
      "additionalProperties": false,
      "properties": {
        "descriptor_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Descriptor Sha256",
          "type": "string"
        },
        "image_digest": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Image Digest",
          "type": "string"
        },
        "implementation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Implementation Id",
          "type": "string"
        },
        "implementation_version": {
          "maxLength": 120,
          "minLength": 1,
          "title": "Implementation Version",
          "type": "string"
        },
        "output_role": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Output Role",
          "type": "string"
        },
        "portable_intent_schema": {
          "$ref": "#/$defs/JsonSchemaDocument"
        },
        "primary_operation_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Primary Operation Contract Sha256",
          "type": "string"
        },
        "primary_operation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Primary Operation Id",
          "type": "string"
        },
        "protocol": {
          "const": "stove0-review-sampler/v1",
          "default": "stove0-review-sampler/v1",
          "title": "Protocol",
          "type": "string"
        },
        "source_revision": {
          "maxLength": 200,
          "minLength": 1,
          "title": "Source Revision",
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
      "title": "SamplerDescriptor",
      "type": "object"
    },
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
    },
    "SamplerRequest": {
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
    "SamplerResult": {
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
        "result_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Result Sha256",
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
        "state",
        "result_sha256"
      ],
      "title": "SamplerResult",
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
    "coverage": {
      "$ref": "#/$defs/SamplerConformanceCoverage"
    },
    "format": {
      "const": "stove0-review-sampler-conformance-result/v1",
      "default": "stove0-review-sampler-conformance-result/v1",
      "title": "Format",
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
      "title": "Sampling",
      "type": "string"
    },
    "status": {
      "enum": [
        "conformant",
        "inspected"
      ],
      "title": "Status",
      "type": "string"
    }
  },
  "required": [
    "status",
    "sampler",
    "coverage",
    "sampling"
  ],
  "title": "SamplerConformanceResult",
  "type": "object"
}
```
