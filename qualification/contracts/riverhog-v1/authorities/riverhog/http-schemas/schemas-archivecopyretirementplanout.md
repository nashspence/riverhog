# schemas: ArchiveCopyRetirementPlanOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-archivecopyretirementplanout:073bedc909 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-05bde79c4b"></a>

- <a id="s-86305dce22"></a>`type`: `"object"`
- <a id="s-b920650ee4"></a>`additionalProperties`: `false`
- <a id="s-2e90ad56f9"></a>`required`: `["status","collection_id","store","warning","expires_at","challenge","target_copy","retained_copies","retired_retrieval_job_count","blockers","verification_note","billing_note"]`
- <a id="s-77e7f36434"></a>`title`: `"ArchiveCopyRetirementPlanOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-662b46ed5a"></a>`billing_note` | yes | type="string"; title="Billing Note" |  |
| <a id="s-3bb1c53f61"></a>`blockers` | yes | type="array"; items=(type="string"); title="Blockers" |  |
| <a id="s-a8bf1cfd6c"></a>`challenge` | yes | anyOf=[(type="string"); (type="null")]; title="Challenge" |  |
| <a id="s-b3e346a57e"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-22bb7f1875"></a>`expires_at` | yes | type="string"; title="Expires At" |  |
| <a id="s-3b329cb5f9"></a>`retained_copies` | yes | type="array"; items=([ArchiveCopyRetirementRetainedOut](schemas-archivecopyretirementretainedout.md)); title="Retained Copies" |  |
| <a id="s-91c234b2f5"></a>`retired_retrieval_job_count` | yes | type="integer"; title="Retired Retrieval Job Count" |  |
| <a id="s-bc86b383bf"></a>`status` | yes | type="string"; enum=["ready","blocked","retiring"]; title="Status" |  |
| <a id="s-ccd5b75997"></a>`store` | yes | [ArchiveStoreName](schemas-archivestorename.md) |  |
| <a id="s-1bc1feaa61"></a>`target_copy` | yes | [ArchiveCopyRetirementTargetOut](schemas-archivecopyretirementtargetout.md) |  |
| <a id="s-0e9fe4a515"></a>`verification_note` | yes | type="string"; title="Verification Note" |  |
| <a id="s-3902eea4d7"></a>`warning` | yes | type="string"; title="Warning" |  |

### Exactly one must match (`oneOf`)

| Alternative | Schema |
|---|---|
| 1 | [See `oneOf` alternative 1](#s-178cb4c49f) |
| 2 | [See `oneOf` alternative 2](#s-bfe171a84c) |

### <a id="s-178cb4c49f"></a>`oneOf` alternative 1


#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-191e6ff8d3"></a>`blockers` | no | minItems=1 |  |
| <a id="s-f234572c77"></a>`challenge` | no | type="null" |  |
| <a id="s-58ffc09adb"></a>`status` | no | const="blocked" |  |

### <a id="s-bfe171a84c"></a>`oneOf` alternative 2


#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-82a7ddf46a"></a>`blockers` | no | maxItems=0 |  |
| <a id="s-453077a665"></a>`challenge` | no | type="string"; minLength=1 |  |
| <a id="s-9d526db619"></a>`status` | no | enum=["ready","retiring"] |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [oneOf alternative 1 · field blockers](#s-191e6ff8d3) | `cardinality · items · operational_policy` | shared above |
| [field blockers](#s-3bb1c53f61) | `cardinality · items · operational_policy` | shared above |
| [field retained_copies](#s-3b329cb5f9) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; reason="state-conditioned-empty-set"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [oneOf alternative 2 · field blockers](#s-82a7ddf46a) | `cardinality · items · contract_max` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [ArchiveCopyRetirementRetainedOut](schemas-archivecopyretirementretainedout.md)
- [ArchiveCopyRetirementTargetOut](schemas-archivecopyretirementtargetout.md)
- [ArchiveStoreName](schemas-archivestorename.md)
- [CollectionId](schemas-collectionid.md)

## Governing policies

- <a id="pa-80614c9394"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-10f3583f07"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-d1ae0a62b6"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyRetirementPlanOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 25fb81614c8184e917269d3bdba07c7340d77e680d89b0fc832b24319e5900e2 -->

```json
{
  "additionalProperties": false,
  "oneOf": [
    {
      "properties": {
        "blockers": {
          "minItems": 1
        },
        "challenge": {
          "type": "null"
        },
        "status": {
          "const": "blocked"
        }
      }
    },
    {
      "properties": {
        "blockers": {
          "maxItems": 0
        },
        "challenge": {
          "minLength": 1,
          "type": "string"
        },
        "status": {
          "enum": [
            "ready",
            "retiring"
          ]
        }
      }
    }
  ],
  "properties": {
    "billing_note": {
      "title": "Billing Note",
      "type": "string"
    },
    "blockers": {
      "items": {
        "type": "string"
      },
      "title": "Blockers",
      "type": "array"
    },
    "challenge": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Challenge"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "expires_at": {
      "title": "Expires At",
      "type": "string"
    },
    "retained_copies": {
      "items": {
        "$ref": "#/components/schemas/ArchiveCopyRetirementRetainedOut"
      },
      "title": "Retained Copies",
      "type": "array"
    },
    "retired_retrieval_job_count": {
      "title": "Retired Retrieval Job Count",
      "type": "integer"
    },
    "status": {
      "enum": [
        "ready",
        "blocked",
        "retiring"
      ],
      "title": "Status",
      "type": "string"
    },
    "store": {
      "$ref": "#/components/schemas/ArchiveStoreName"
    },
    "target_copy": {
      "$ref": "#/components/schemas/ArchiveCopyRetirementTargetOut"
    },
    "verification_note": {
      "title": "Verification Note",
      "type": "string"
    },
    "warning": {
      "title": "Warning",
      "type": "string"
    }
  },
  "required": [
    "status",
    "collection_id",
    "store",
    "warning",
    "expires_at",
    "challenge",
    "target_copy",
    "retained_copies",
    "retired_retrieval_job_count",
    "blockers",
    "verification_note",
    "billing_note"
  ],
  "title": "ArchiveCopyRetirementPlanOut",
  "type": "object"
}
```

</details>
