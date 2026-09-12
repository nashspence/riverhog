# schemas: IncompleteArchiveCopyOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-incompletearchivecopyout:08296c964e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `title`: IncompleteArchiveCopyOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `archive_root` | yes | anyOf=#/components/schemas/PendingArchiveRootPublicationOut \| #/components/schemas/UploadedArchiveRootPublicationOut |  |
| `failure` | yes | type="null" |  |
| `last_uploaded_at` | yes | anyOf=type="string" \| type="null" |  |
| `last_verified_at` | yes | anyOf=type="string" \| type="null" |  |
| `object_count` | yes | type="integer"; minimum=0 |  |
| `state` | yes | type="string"; enum=["pending","uploading","retrying"] |  |
| `storage_prefix` | yes | anyOf=type="string"; minLength=1 \| type="null" |  |
| `store` | yes | #/components/schemas/ArchiveStoreName |  |
| `stored_bytes` | yes | type="integer"; minimum=0 |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArchiveStoreName](schemas-archivestorename.md)
- [schemas: PendingArchiveRootPublicationOut](schemas-pendingarchiverootpublicationout.md)
- [schemas: UploadedArchiveRootPublicationOut](schemas-uploadedarchiverootpublicationout.md)

## Governing policies

- `compatibility/http-api/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/IncompleteArchiveCopyOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 03f626031ce0ea3a25b02a5259193a1083c2047db6f52e8e72ae31d6c7c24e5a -->

```json
{
  "additionalProperties": false,
  "properties": {
    "archive_root": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/PendingArchiveRootPublicationOut"
        },
        {
          "$ref": "#/components/schemas/UploadedArchiveRootPublicationOut"
        }
      ],
      "title": "Archive Root"
    },
    "failure": {
      "title": "Failure",
      "type": "null"
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
      "enum": [
        "pending",
        "uploading",
        "retrying"
      ],
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
  "title": "IncompleteArchiveCopyOut",
  "type": "object"
}
```
