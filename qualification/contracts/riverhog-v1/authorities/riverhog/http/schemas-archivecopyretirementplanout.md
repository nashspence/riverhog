# schemas: ArchiveCopyRetirementPlanOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-archivecopyretirementplanout:3f3e4bb826 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

<a id="s-05bde79c4b16"></a>
- <a id="s-77e7f36434c7"></a>`title`: ArchiveCopyRetirementPlanOut
- <a id="s-86305dce22f7"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-662b46ed5ade"></a>`billing_note` | yes | type="string" |  |
| <a id="s-3bb1c53f6164"></a>`blockers` | yes | type="array"; items=(type="string") |  |
| <a id="s-a8bf1cfd6cbb"></a>`challenge` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-b3e346a57eca"></a>`collection_id` | yes | #/components/schemas/CollectionId |  |
| <a id="s-22bb7f18753e"></a>`expires_at` | yes | type="string" |  |
| <a id="s-3b329cb5f913"></a>`retained_copies` | yes | type="array"; items=(#/components/schemas/ArchiveCopyRetirementRetainedOut) |  |
| <a id="s-91c234b2f55a"></a>`retired_retrieval_job_count` | yes | type="integer" |  |
| <a id="s-bc86b383bfc5"></a>`status` | yes | type="string"; enum=["ready","blocked","retiring"] |  |
| <a id="s-ccd5b75997f4"></a>`store` | yes | #/components/schemas/ArchiveStoreName |  |
| <a id="s-1bc1feaa6127"></a>`target_copy` | yes | #/components/schemas/ArchiveCopyRetirementTargetOut |  |
| <a id="s-0e9fe4a51590"></a>`verification_note` | yes | type="string" |  |
| <a id="s-3902eea4d7b9"></a>`warning` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-191e6ff8d3b2"></a>oneOf alternative 1 · field blockers | `cardinality · items · operational_policy` | shared above |
| [field blockers](#s-3bb1c53f6164) | `cardinality · items · operational_policy` | shared above |
| [field retained_copies](#s-3b329cb5f913) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=0; reason="state-conditioned-empty-set"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-82a7ddf46a96"></a>oneOf alternative 2 · field blockers | `cardinality · items · contract_max` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArchiveCopyRetirementRetainedOut](schemas-archivecopyretirementretainedout.md)
- [schemas: ArchiveCopyRetirementTargetOut](schemas-archivecopyretirementtargetout.md)
- [schemas: ArchiveStoreName](schemas-archivestorename.md)
- [schemas: CollectionId](schemas-collectionid.md)

## Governing policies

- <a id="pa-164587cdabdc"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-5ece69d65b8c"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)
- <a id="pa-6571e683dca4"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyRetirementPlanOut`

### Exact owned JSON

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
