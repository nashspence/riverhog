# schemas: ArchiveCopyJobOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-archivecopyjobout:4841696744 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyJobOut`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ApplicationKeyId](schemas-applicationkeyid.md)
- [schemas: ApplicationName](schemas-applicationname.md)
- [schemas: ArchiveCopyState](schemas-archivecopystate.md)
- [schemas: ArchiveStoreName](schemas-archivestorename.md)
- [schemas: CollectionId](schemas-collectionid.md)

## Contract summary

- `title`: ArchiveCopyJobOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `collection_id` | yes | #/components/schemas/CollectionId |  |
| `completed_at` | yes | object (2 fields) |  |
| `destination_store` | yes | #/components/schemas/ArchiveStoreName |  |
| `expires_at` | yes | object (2 fields) |  |
| `failure` | yes | object (2 fields) |  |
| `initiated_by_app` | yes | object (1 fields) |  |
| `initiated_by_key_id` | yes | object (1 fields) |  |
| `ready_at` | yes | object (2 fields) |  |
| `requested_at` | yes | object (2 fields) |  |
| `source_store` | yes | object (1 fields) |  |
| `state` | yes | #/components/schemas/ArchiveCopyState |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 29dc4b874f692462c8d48922f23cd8d427114f94ade134c50fba648edc4b7427 -->

```json
{
  "additionalProperties": false,
  "allOf": [
    {
      "else": {
        "properties": {
          "completed_at": {
            "type": "null"
          }
        }
      },
      "if": {
        "properties": {
          "state": {
            "enum": [
              "completed",
              "canceled"
            ]
          }
        }
      },
      "then": {
        "properties": {
          "completed_at": {
            "type": "string"
          }
        }
      }
    },
    {
      "else": {
        "properties": {
          "failure": {
            "type": "null"
          }
        }
      },
      "if": {
        "properties": {
          "state": {
            "const": "failed"
          }
        }
      },
      "then": {
        "properties": {
          "failure": {
            "minLength": 1,
            "type": "string"
          }
        }
      }
    }
  ],
  "properties": {
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "completed_at": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Completed At"
    },
    "destination_store": {
      "$ref": "#/components/schemas/ArchiveStoreName"
    },
    "expires_at": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Expires At"
    },
    "failure": {
      "anyOf": [
        {
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Failure"
    },
    "initiated_by_app": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ApplicationName"
        },
        {
          "type": "null"
        }
      ]
    },
    "initiated_by_key_id": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ApplicationKeyId"
        },
        {
          "type": "null"
        }
      ]
    },
    "ready_at": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Ready At"
    },
    "requested_at": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Requested At"
    },
    "source_store": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ArchiveStoreName"
        },
        {
          "type": "null"
        }
      ]
    },
    "state": {
      "$ref": "#/components/schemas/ArchiveCopyState"
    }
  },
  "required": [
    "collection_id",
    "source_store",
    "destination_store",
    "initiated_by_app",
    "initiated_by_key_id",
    "state",
    "requested_at",
    "ready_at",
    "expires_at",
    "completed_at",
    "failure"
  ],
  "title": "ArchiveCopyJobOut",
  "type": "object"
}
```
