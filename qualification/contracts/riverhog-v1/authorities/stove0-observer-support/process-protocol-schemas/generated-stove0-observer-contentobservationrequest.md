# generated:stove0-observer: ContentObservationRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:stove0-observer-support:generated-stove0-observer-contentobservationrequest:16a3bb305d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-e56e44e596"></a>

- <a id="s-29f0f828ab"></a>`type`: `"object"`
- <a id="s-ecb67e8126"></a>`additionalProperties`: `false`
- <a id="s-a814a3ed78"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`
- <a id="s-ee1f90bd8d"></a>`title`: `"ContentObservationRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e00ca214b4"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1"; title="Format" |  |
| <a id="s-0177d54193"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576; title="Maximum Result Bytes" |  |
| <a id="s-06b40864ae"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Observer Contract Id" |  |
| <a id="s-496661ccc0"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Observer Contract Sha256" |  |
| <a id="s-7efb2870c6"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Observer Descriptor Sha256" |  |
| <a id="s-a9c4f78fcc"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$"; title="Observer Registration Id" |  |
| <a id="s-e9eb5a1bc7"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-78cce5ae7a)); title="Options" |  |
| <a id="s-75c22b7817"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Id" |  |
| <a id="s-9e8521e08b"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only"; title="Retrieval Policy" |  |
| <a id="s-3a60352800"></a>`subjects` | yes | type="array"; items=([WorkArtifactSubject](#s-a9cdf4c84d)); minItems=1; title="Subjects" |  |
| <a id="s-6698c24300"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300; title="Timeout Seconds" |  |
| <a id="s-d2832d9ecc"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Work Id" |  |

### Definitions

- [CollectionId](#s-fdcd4e1424)
- [CollectionRootIdentityRef](#s-5eb8e0e022)
- [JsonValue](#s-78cce5ae7a)
- [WorkArtifactSubject](#s-a9cdf4c84d)

### <a id="s-fdcd4e1424"></a>definition `CollectionId`


#### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-49c9d0e73a"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-76e76569a0"></a>2 | not=(const="0") |

### <a id="s-5eb8e0e022"></a>definition `CollectionRootIdentityRef`

- <a id="s-5fd80b63df"></a>`type`: `"object"`
- <a id="s-c31bebd0f4"></a>`additionalProperties`: `false`
- <a id="s-135b193faf"></a>`description`: `"Embedded Stove0 reference to the Riverhog collection-root identity."`
- <a id="s-fd72aac737"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`
- <a id="s-3ab054f7ea"></a>`title`: `"CollectionRootIdentityRef"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5c4c56097c"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Archive Root Sha256" |  |
| <a id="s-2c7b907058"></a>`collection_id` | yes | [CollectionId](#s-fdcd4e1424) |  |
| <a id="s-fc784f6350"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Content Identity" |  |

### <a id="s-78cce5ae7a"></a>definition `JsonValue`

- Accepts: any JSON value.

### <a id="s-a9cdf4c84d"></a>definition `WorkArtifactSubject`

- <a id="s-940a24317a"></a>`type`: `"object"`
- <a id="s-3e11ad963d"></a>`additionalProperties`: `false`
- <a id="s-6fea4aaff4"></a>`description`: `"A collection logical file assigned an ID and role within one Stove0 work."`
- <a id="s-3e35a988b9"></a>`required`: `["id","role","collection","path","bytes","sha256"]`
- <a id="s-a67b6592e7"></a>`title`: `"WorkArtifactSubject"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-909ae5b35e"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-00b0608ea6"></a>`collection` | yes | [CollectionRootIdentityRef](#s-5eb8e0e022) |  |
| <a id="s-5e2690235f"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"; title="Id" |  |
| <a id="s-489163f311"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null; title="Media Type" |  |
| <a id="s-f8a688e861"></a>`path` | yes | type="string"; maxLength=4096; minLength=1; title="Path" |  |
| <a id="s-fc77ddcfa4"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |
| <a id="s-b751ed7d32"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-observer-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field options](#s-e9eb5a1bc7) | `cardinality · entries · operational_policy` | shared above |
| [field subjects](#s-3a60352800) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field maximum_result_bytes](#s-0177d54193) | `value · schema-value · contract_max` | maximum=67108864; minimum=1; reason="schema-maximum" |
| [field observer_contract_sha256](#s-496661ccc0) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field observer_descriptor_sha256](#s-7efb2870c6) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field request_id](#s-75c22b7817) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field timeout_seconds](#s-6698c24300) | `value · schema-value · contract_max` | maximum=86400; minimum=1; reason="schema-maximum" |
| [field work_id](#s-d2832d9ecc) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Related interface records

- [generated:stove0-observer protocol](../process-protocol/generated-stove0-observer-protocol.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-03a67912f3"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-ed9360c42d"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-4d8d438517"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:stove0-observer](../../../evidence/sources/authorities.md#src-dcc0b5485b) — [some-implementations/stove0/packages/observer-support/src/stove0\_observer\_support/schemas.py::observer\_schema\_bundle](../../../../../../some-implementations/stove0/packages/observer-support/src/stove0_observer_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-observer/schemas/ContentObservationRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7a3d65096fa69762163c34d20222b33709ff66b1edd543ae415eec415b62730c -->

```json
{
  "$defs": {
    "CollectionId": {
      "allOf": [
        {
          "pattern": "^(?:0|[1-9][0-9]{0,17}|[1-8][0-9]{18}|9[0-1][0-9]{17}|92[0-1][0-9]{16}|922[0-2][0-9]{15}|9223[0-2][0-9]{14}|92233[0-6][0-9]{13}|922337[0-1][0-9]{12}|92233720[0-2][0-9]{10}|922337203[0-5][0-9]{9}|9223372036[0-7][0-9]{8}|92233720368[0-4][0-9]{7}|922337203685[0-3][0-9]{6}|9223372036854[0-6][0-9]{5}|92233720368547[0-6][0-9]{4}|922337203685477[0-4][0-9]{3}|9223372036854775[0-7][0-9]{2}|922337203685477580[0-6][0-9]{0}|9223372036854775807)(?![\\s\\S])",
          "type": "string"
        },
        {
          "not": {
            "const": "0"
          }
        }
      ]
    },
    "CollectionRootIdentityRef": {
      "additionalProperties": false,
      "description": "Embedded Stove0 reference to the Riverhog collection-root identity.",
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
      "title": "CollectionRootIdentityRef",
      "type": "object"
    },
    "JsonValue": {},
    "WorkArtifactSubject": {
      "additionalProperties": false,
      "description": "A collection logical file assigned an ID and role within one Stove0 work.",
      "properties": {
        "bytes": {
          "minimum": 0,
          "title": "Bytes",
          "type": "integer"
        },
        "collection": {
          "$ref": "#/$defs/CollectionRootIdentityRef"
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
      "title": "WorkArtifactSubject",
      "type": "object"
    }
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
        "$ref": "#/$defs/WorkArtifactSubject"
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
  "title": "ContentObservationRequest",
  "type": "object"
}
```

</details>
