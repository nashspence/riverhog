# schemas: CollectionDeletionPlanOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectiondeletionplanout:21e8fd2ce3 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 6 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionDeletionPlanOut`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: CollectionDeletionArchiveCopyOut](schemas-collectiondeletionarchivecopyout.md)
- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: RetirementClaimReferenceDocument](schemas-retirementclaimreferencedocument.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `contract_max` | maximum=0, reason=state-conditioned-empty-set |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `contract_max` | maximum=55, reason=bounded-diagnostic-sample-with-explicit-overflow-markers |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract summary

- `title`: CollectionDeletionPlanOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `archive_copies` | yes | array |  |
| `archive_object_count` | yes | integer |  |
| `billing_note` | yes | string |  |
| `blockers` | yes | array |  |
| `bytes` | yes | integer |  |
| `challenge` | yes | object (2 fields) |  |
| `collection_id` | yes | #/components/schemas/CollectionId |  |
| `expires_at` | yes | string |  |
| `file_count` | yes | integer |  |
| `inventory_identity` | yes | string |  |
| `metadata_rows` | yes | object |  |
| `remote_storage_bytes` | yes | integer |  |
| `retirement_claim` | no | object (1 fields) |  |
| `status` | yes | string |  |
| `upload_file_count` | yes | integer |  |
| `warning` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c45b1e900ac654c02b79b444cd1069c7afa157fcd5e00f8632f6b6b9c6adb342 -->

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
            "deleting"
          ]
        }
      }
    }
  ],
  "properties": {
    "archive_copies": {
      "items": {
        "$ref": "#/components/schemas/CollectionDeletionArchiveCopyOut"
      },
      "title": "Archive Copies",
      "type": "array"
    },
    "archive_object_count": {
      "title": "Archive Object Count",
      "type": "integer"
    },
    "billing_note": {
      "title": "Billing Note",
      "type": "string"
    },
    "blockers": {
      "items": {
        "type": "string"
      },
      "maxItems": 55,
      "title": "Blockers",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "contract_max",
        "reason": "bounded-diagnostic-sample-with-explicit-overflow-markers"
      }
    },
    "bytes": {
      "title": "Bytes",
      "type": "integer"
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
    "file_count": {
      "title": "File Count",
      "type": "integer"
    },
    "inventory_identity": {
      "title": "Inventory Identity",
      "type": "string"
    },
    "metadata_rows": {
      "additionalProperties": {
        "type": "integer"
      },
      "title": "Metadata Rows",
      "type": "object"
    },
    "remote_storage_bytes": {
      "title": "Remote Storage Bytes",
      "type": "integer"
    },
    "retirement_claim": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/RetirementClaimReferenceDocument"
        },
        {
          "type": "null"
        }
      ]
    },
    "status": {
      "enum": [
        "ready",
        "blocked",
        "deleting"
      ],
      "title": "Status",
      "type": "string"
    },
    "upload_file_count": {
      "title": "Upload File Count",
      "type": "integer"
    },
    "warning": {
      "title": "Warning",
      "type": "string"
    }
  },
  "required": [
    "status",
    "collection_id",
    "warning",
    "expires_at",
    "challenge",
    "file_count",
    "bytes",
    "archive_copies",
    "archive_object_count",
    "remote_storage_bytes",
    "upload_file_count",
    "inventory_identity",
    "metadata_rows",
    "blockers",
    "billing_note"
  ],
  "title": "CollectionDeletionPlanOut",
  "type": "object"
}
```
