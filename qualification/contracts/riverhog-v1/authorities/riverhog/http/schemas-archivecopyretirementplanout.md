# schemas: ArchiveCopyRetirementPlanOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-archivecopyretirementplanout:3f3e4bb826 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

- `title`: ArchiveCopyRetirementPlanOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `billing_note` | yes | type="string" |  |
| `blockers` | yes | type="array"; items=(type="string") |  |
| `challenge` | yes | anyOf=type="string" \| type="null" |  |
| `collection_id` | yes | #/components/schemas/CollectionId |  |
| `expires_at` | yes | type="string" |  |
| `retained_copies` | yes | type="array"; items=(#/components/schemas/ArchiveCopyRetirementRetainedOut) |  |
| `retired_retrieval_job_count` | yes | type="integer" |  |
| `status` | yes | type="string"; enum=["ready","blocked","retiring"] |  |
| `store` | yes | #/components/schemas/ArchiveStoreName |  |
| `target_copy` | yes | #/components/schemas/ArchiveCopyRetirementTargetOut |  |
| `verification_note` | yes | type="string" |  |
| `warning` | yes | type="string" |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `contract_max` | maximum=0, reason=state-conditioned-empty-set |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArchiveCopyRetirementRetainedOut](schemas-archivecopyretirementretainedout.md)
- [schemas: ArchiveCopyRetirementTargetOut](schemas-archivecopyretirementtargetout.md)
- [schemas: ArchiveStoreName](schemas-archivestorename.md)
- [schemas: CollectionId](schemas-collectionid.md)

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
