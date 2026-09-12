# generated:stove0-observer: ObservationInvocation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:stove0-observer-support:generated-stove0-observer-observationinvocation:bbcbfd5cd0 -->

Fence-bound invocation authority excluded from semantic request identity.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](index.md#f-d47202b295) |
| Contract elements | 1 |
| Extent decisions | 17 |

## External contract

<a id="s-5cc4b3adc3"></a>
- <a id="s-d5a41e9392"></a>`title`: ObservationInvocation
- <a id="s-10ba72f37a"></a>`description`: Fence-bound invocation authority excluded from semantic request identity.
- <a id="s-3a530566b1"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-072f4b544c"></a>`claim_id` | yes | type="string"; minLength=1; maxLength=160 |  |
| <a id="s-d483a59ad8"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-5aa86a4371"></a>`request` | yes | #/$defs/ObservationRequest |  |
| <a id="s-0109a56940"></a>`runtime` | yes | #/$defs/ObserverRuntimeAuthority |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-6ecabdecfa"></a>`ArtifactSubject` | type="object"; fields=`bytes`, `collection`, `id`, `media_type`, `path`, `role`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-90dcdf64ae"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-ad3843b90a"></a>`CollectionRootRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`; additional keys=`additionalProperties`, `required` |
| <a id="s-95de9a9e67"></a>`JsonValue` | empty object |
| <a id="s-47065fc11b"></a>`ObservationRequest` | type="object"; fields=`format`, `maximum_result_bytes`, `observer_contract_id`, `observer_contract_sha256`, `observer_descriptor_sha256`, `observer_registration_id`, `options`, `request_id`, `retrieval_policy`, `subjects`, `timeout_seconds`, `work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-f89907f0ff"></a>`ObserverRuntimeAuthority` | type="object"; fields=`allow_insecure_http`, `capability_token`, `riverhog_base_url`, `transport`, `workspace_assurance`; additional keys=`additionalProperties`, `required` |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-observer-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-2a086b4455"></a>[definition ArtifactSubject · field bytes](#s-6ecabdecfa) | `value · schema-value · operational_policy` | shared above |
| <a id="s-df3b33cb99"></a>[definition ObservationRequest · field options](#s-47065fc11b) | `cardinality · entries · operational_policy` | shared above |
| <a id="s-433a287632"></a>[definition ObservationRequest · field subjects](#s-47065fc11b) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field claim_id](#s-072f4b544c) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| <a id="s-775d0ab059"></a>[definition ArtifactSubject · field media_type · string value](#s-6ecabdecfa) | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| <a id="s-19b80a5714"></a>[definition ArtifactSubject · field path](#s-6ecabdecfa) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| <a id="s-bc561903c6"></a>[definition ArtifactSubject · field sha256](#s-6ecabdecfa) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-7de457ad21"></a>[definition CollectionRootRef · field archive_root_sha256](#s-ad3843b90a) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-13ae4d5efc"></a>[definition CollectionRootRef · field content_identity](#s-ad3843b90a) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-938eb9b92c"></a>[definition ObservationRequest · field maximum_result_bytes](#s-47065fc11b) | `value · schema-value · contract_max` | maximum=67108864; minimum=1; reason="schema-maximum" |
| <a id="s-01059693d6"></a>[definition ObservationRequest · field observer_contract_sha256](#s-47065fc11b) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-df85f4c9be"></a>[definition ObservationRequest · field observer_descriptor_sha256](#s-47065fc11b) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-25438bc744"></a>[definition ObservationRequest · field request_id](#s-47065fc11b) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-a884a49972"></a>[definition ObservationRequest · field timeout_seconds](#s-47065fc11b) | `value · schema-value · contract_max` | maximum=86400; minimum=1; reason="schema-maximum" |
| <a id="s-e81ed70f43"></a>[definition ObservationRequest · field work_id](#s-47065fc11b) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-a17c050adb"></a>[definition ObserverRuntimeAuthority · field capability_token](#s-f89907f0ff) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| <a id="s-aea2c68d3e"></a>[definition ObserverRuntimeAuthority · field riverhog_base_url](#s-f89907f0ff) | `length · characters · contract_max` | maximum=2048; minimum=1; reason="schema-maximum" |

## Governing policies

- <a id="pa-80dec768f4"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-e7e02346fb"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-a45b27dea2"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:stove0-observer](../../../evidence/sources.md#src-dcc0b5485b) — `reference/stove0/packages/observer-support/src/stove0_observer_support/schemas.py::observer_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-observer/schemas/ObservationInvocation`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 82cb342e57cc6456dc2d8acdb09d8836f7be2d23c0d8d5ab5ce8d23a5d8c32d7 -->

```json
{
  "$defs": {
    "ArtifactSubject": {
      "additionalProperties": false,
      "properties": {
        "bytes": {
          "minimum": 0,
          "title": "Bytes",
          "type": "integer"
        },
        "collection": {
          "$ref": "#/$defs/CollectionRootRef"
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
        "role": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Role",
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
        "role",
        "collection",
        "path",
        "bytes",
        "sha256"
      ],
      "title": "ArtifactSubject",
      "type": "object"
    },
    "CollectionId": {
      "minimum": 1,
      "type": "integer"
    },
    "CollectionRootRef": {
      "additionalProperties": false,
      "properties": {
        "archive_root_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Archive Root Sha256",
          "type": "string"
        },
        "collection_id": {
          "$ref": "#/$defs/CollectionId"
        },
        "content_identity": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Content Identity",
          "type": "string"
        }
      },
      "required": [
        "collection_id",
        "archive_root_sha256",
        "content_identity"
      ],
      "title": "CollectionRootRef",
      "type": "object"
    },
    "JsonValue": {},
    "ObservationRequest": {
      "additionalProperties": false,
      "properties": {
        "format": {
          "const": "stove0-observation-request/v1",
          "default": "stove0-observation-request/v1",
          "title": "Format",
          "type": "string"
        },
        "maximum_result_bytes": {
          "default": 1048576,
          "maximum": 67108864,
          "minimum": 1,
          "title": "Maximum Result Bytes",
          "type": "integer"
        },
        "observer_contract_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Observer Contract Id",
          "type": "string"
        },
        "observer_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Observer Contract Sha256",
          "type": "string"
        },
        "observer_descriptor_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Observer Descriptor Sha256",
          "type": "string"
        },
        "observer_registration_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$",
          "title": "Observer Registration Id",
          "type": "string"
        },
        "options": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Options",
          "type": "object"
        },
        "request_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Request Id",
          "type": "string"
        },
        "retrieval_policy": {
          "default": "available-only",
          "enum": [
            "available-only",
            "allow"
          ],
          "title": "Retrieval Policy",
          "type": "string"
        },
        "subjects": {
          "items": {
            "$ref": "#/$defs/ArtifactSubject"
          },
          "minItems": 1,
          "title": "Subjects",
          "type": "array"
        },
        "timeout_seconds": {
          "default": 300,
          "maximum": 86400,
          "minimum": 1,
          "title": "Timeout Seconds",
          "type": "integer"
        },
        "work_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Work Id",
          "type": "string"
        }
      },
      "required": [
        "work_id",
        "observer_registration_id",
        "observer_descriptor_sha256",
        "observer_contract_id",
        "observer_contract_sha256",
        "subjects",
        "request_id"
      ],
      "title": "ObservationRequest",
      "type": "object"
    },
    "ObserverRuntimeAuthority": {
      "additionalProperties": false,
      "description": "Secret-bearing invocation material excluded from durable request identity.",
      "properties": {
        "allow_insecure_http": {
          "default": false,
          "title": "Allow Insecure Http",
          "type": "boolean"
        },
        "capability_token": {
          "maxLength": 4096,
          "minLength": 1,
          "title": "Capability Token",
          "type": "string"
        },
        "riverhog_base_url": {
          "maxLength": 2048,
          "minLength": 1,
          "title": "Riverhog Base Url",
          "type": "string"
        },
        "transport": {
          "const": "riverhog-capability/v1",
          "default": "riverhog-capability/v1",
          "title": "Transport",
          "type": "string"
        },
        "workspace_assurance": {
          "enum": [
            "encrypted",
            "ephemeral"
          ],
          "title": "Workspace Assurance",
          "type": "string"
        }
      },
      "required": [
        "riverhog_base_url",
        "capability_token",
        "workspace_assurance"
      ],
      "title": "ObserverRuntimeAuthority",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "description": "Fence-bound invocation authority excluded from semantic request identity.",
  "properties": {
    "claim_id": {
      "maxLength": 160,
      "minLength": 1,
      "title": "Claim Id",
      "type": "string"
    },
    "fence": {
      "minimum": 1,
      "title": "Fence",
      "type": "integer"
    },
    "request": {
      "$ref": "#/$defs/ObservationRequest"
    },
    "runtime": {
      "$ref": "#/$defs/ObserverRuntimeAuthority"
    }
  },
  "required": [
    "request",
    "claim_id",
    "fence",
    "runtime"
  ],
  "title": "ObservationInvocation",
  "type": "object"
}
```
