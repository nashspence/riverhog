# generated:stove0-observer: ObservationRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:stove0-observer-support:generated-stove0-observer-observationrequest:e3ca0f7b08 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-648efb2729"></a>

- <a id="s-5731607bad"></a>`type`: `"object"`
- <a id="s-5b729f0e07"></a>`additionalProperties`: `false`
- <a id="s-841e0443cb"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`
- <a id="s-ac74832637"></a>`title`: `"ObservationRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1a84ef3fb6"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1"; title="Format" |  |
| <a id="s-116a37912c"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576; title="Maximum Result Bytes" |  |
| <a id="s-cb3089e46f"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Observer Contract Id" |  |
| <a id="s-73d893643f"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Observer Contract Sha256" |  |
| <a id="s-b069094596"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Observer Descriptor Sha256" |  |
| <a id="s-c4054e4812"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$"; title="Observer Registration Id" |  |
| <a id="s-20284f8604"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-0d24c87770)); title="Options" |  |
| <a id="s-4d97eaf49f"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Id" |  |
| <a id="s-a2e65475da"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only"; title="Retrieval Policy" |  |
| <a id="s-255eaccaee"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-5f3cf64beb)); minItems=1; title="Subjects" |  |
| <a id="s-14e8b087af"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300; title="Timeout Seconds" |  |
| <a id="s-f786831d72"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Work Id" |  |

### Definitions

- [ArtifactSubject](#s-5f3cf64beb)
- [CollectionId](#s-6c41954745)
- [CollectionRootRef](#s-6505b70cc1)
- [JsonValue](#s-0d24c87770)

### <a id="s-5f3cf64beb"></a>definition `ArtifactSubject`

- <a id="s-cb745cb469"></a>`type`: `"object"`
- <a id="s-9b5e1fa3a0"></a>`additionalProperties`: `false`
- <a id="s-feca5d64d0"></a>`required`: `["id","role","collection","path","bytes","sha256"]`
- <a id="s-56ccbd2fe0"></a>`title`: `"ArtifactSubject"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-04fe4b8d63"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-27737309c3"></a>`collection` | yes | [CollectionRootRef](#s-6505b70cc1) |  |
| <a id="s-59af549a0d"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"; title="Id" |  |
| <a id="s-3fdd710c98"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null; title="Media Type" |  |
| <a id="s-9c6514fe4f"></a>`path` | yes | type="string"; maxLength=4096; minLength=1; title="Path" |  |
| <a id="s-10526cbd05"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |
| <a id="s-87837638c1"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### <a id="s-6c41954745"></a>definition `CollectionId`

- <a id="s-c8e3ee95f9"></a>`type`: `"integer"`
- <a id="s-a510cf9e90"></a>`minimum`: `1`

### <a id="s-6505b70cc1"></a>definition `CollectionRootRef`

- <a id="s-5329c43ddb"></a>`type`: `"object"`
- <a id="s-e6191c5566"></a>`additionalProperties`: `false`
- <a id="s-90c78cd60f"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`
- <a id="s-3bc9eca261"></a>`title`: `"CollectionRootRef"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-82ff44e246"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Archive Root Sha256" |  |
| <a id="s-30bbe70a20"></a>`collection_id` | yes | [CollectionId](#s-6c41954745) |  |
| <a id="s-7da01f8676"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Content Identity" |  |

### <a id="s-0d24c87770"></a>definition `JsonValue`

- Accepts: any JSON value.

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-observer-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field options](#s-20284f8604) | `cardinality · entries · operational_policy` | shared above |
| [field subjects](#s-255eaccaee) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field maximum_result_bytes](#s-116a37912c) | `value · schema-value · contract_max` | maximum=67108864; minimum=1; reason="schema-maximum" |
| [field observer_contract_sha256](#s-73d893643f) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field observer_descriptor_sha256](#s-b069094596) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field request_id](#s-4d97eaf49f) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field timeout_seconds](#s-14e8b087af) | `value · schema-value · contract_max` | maximum=86400; minimum=1; reason="schema-maximum" |
| [field work_id](#s-f786831d72) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Related interface records

- [generated:stove0-observer protocol](../process-protocol/generated-stove0-observer-protocol.md)

## Governing policies

- <a id="pa-299b887fab"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-345f3c0e70"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-dfe57b3bec"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:stove0-observer](../../../evidence/sources.md#src-dcc0b5485b) — `reference/stove0/packages/observer-support/src/stove0_observer_support/schemas.py::observer_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-observer/schemas/ObservationRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 35ac3c22e749ed4adca72bf8e0e2afea59cd948e7b36f0950b0124f574e5ab28 -->

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
    "JsonValue": {}
  },
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
}
```

</details>
