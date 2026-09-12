# schemas: CollectionUploadDiscardPlanOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploaddiscardplanout:00717f2bec -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadDiscardPlanOut`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: CompleteCollectionUploadCustodyOut](schemas-completecollectionuploadcustodyout.md)
- [schemas: PendingCollectionUploadCustodyOut](schemas-pendingcollectionuploadcustodyout.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract summary

- `title`: CollectionUploadDiscardPlanOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `archive_objects` | yes | integer |  |
| `blockers` | yes | array |  |
| `bytes` | yes | integer |  |
| `challenge` | yes | object (2 fields) |  |
| `collection_id` | yes | #/components/schemas/CollectionId |  |
| `custody` | yes | object (3 fields) |  |
| `expires_at` | yes | string |  |
| `files` | yes | integer |  |
| `state` | yes | string |  |
| `status` | yes | string |  |
| `warning` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9ae8fa9763c1acfe4a0bfaaef8e14e81bff0bc9722261d9ac71ab379ba5cd047 -->

```json
{
  "additionalProperties": false,
  "allOf": [
    {
      "if": {
        "properties": {
          "state": {
            "const": "finalizing"
          }
        },
        "required": [
          "state"
        ]
      },
      "then": {
        "properties": {
          "custody": {
            "properties": {
              "state": {
                "const": "complete"
              }
            },
            "required": [
              "state"
            ]
          }
        }
      }
    }
  ],
  "properties": {
    "archive_objects": {
      "title": "Archive Objects",
      "type": "integer"
    },
    "blockers": {
      "items": {
        "type": "string"
      },
      "title": "Blockers",
      "type": "array"
    },
    "bytes": {
      "minimum": 0,
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
    "custody": {
      "discriminator": {
        "mapping": {
          "complete": "#/components/schemas/CompleteCollectionUploadCustodyOut",
          "pending": "#/components/schemas/PendingCollectionUploadCustodyOut"
        },
        "propertyName": "state"
      },
      "oneOf": [
        {
          "$ref": "#/components/schemas/PendingCollectionUploadCustodyOut"
        },
        {
          "$ref": "#/components/schemas/CompleteCollectionUploadCustodyOut"
        }
      ],
      "title": "Custody"
    },
    "expires_at": {
      "title": "Expires At",
      "type": "string"
    },
    "files": {
      "minimum": 0,
      "title": "Files",
      "type": "integer"
    },
    "state": {
      "enum": [
        "open",
        "closing",
        "uploading",
        "finalizing",
        "orphaned",
        "discarding"
      ],
      "title": "State",
      "type": "string"
    },
    "status": {
      "enum": [
        "ready",
        "blocked"
      ],
      "title": "Status",
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
    "warning",
    "expires_at",
    "challenge",
    "state",
    "files",
    "bytes",
    "custody",
    "archive_objects",
    "blockers"
  ],
  "title": "CollectionUploadDiscardPlanOut",
  "type": "object"
}
```
