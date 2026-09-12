# schemas: FailedArchiveCopyOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-failedarchivecopyout:069daa6dc4 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/FailedArchiveCopyOut`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ArchiveStoreName](schemas-archivestorename.md)
- [schemas: FailedArchiveRootPublicationOut](schemas-failedarchiverootpublicationout.md)

## Contract summary

- `title`: FailedArchiveCopyOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `archive_root` | yes | #/components/schemas/FailedArchiveRootPublicationOut |  |
| `failure` | yes | string |  |
| `last_uploaded_at` | yes | object (2 fields) |  |
| `last_verified_at` | yes | object (2 fields) |  |
| `object_count` | yes | integer |  |
| `state` | yes | string |  |
| `storage_prefix` | yes | object (2 fields) |  |
| `store` | yes | #/components/schemas/ArchiveStoreName |  |
| `stored_bytes` | yes | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3a6f0d8671c86982c2022d1fa4ac56abd480f73282ef86af421e4576ad589c74 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "archive_root": {
      "$ref": "#/components/schemas/FailedArchiveRootPublicationOut"
    },
    "failure": {
      "minLength": 1,
      "title": "Failure",
      "type": "string"
    },
    "last_uploaded_at": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Last Uploaded At"
    },
    "last_verified_at": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Last Verified At"
    },
    "object_count": {
      "minimum": 0,
      "title": "Object Count",
      "type": "integer"
    },
    "state": {
      "const": "failed",
      "title": "State",
      "type": "string"
    },
    "storage_prefix": {
      "anyOf": [
        {
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Storage Prefix"
    },
    "store": {
      "$ref": "#/components/schemas/ArchiveStoreName"
    },
    "stored_bytes": {
      "minimum": 0,
      "title": "Stored Bytes",
      "type": "integer"
    }
  },
  "required": [
    "store",
    "storage_prefix",
    "object_count",
    "stored_bytes",
    "last_uploaded_at",
    "last_verified_at",
    "archive_root",
    "state",
    "failure"
  ],
  "title": "FailedArchiveCopyOut",
  "type": "object"
}
```
