# schemas: UploadedArchiveCopyOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-uploadedarchivecopyout:e859b617b5 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/UploadedArchiveCopyOut`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ArchiveStoreName](schemas-archivestorename.md)
- [schemas: UploadedArchiveRootPublicationOut](schemas-uploadedarchiverootpublicationout.md)

## Contract summary

- `title`: UploadedArchiveCopyOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `archive_root` | yes | #/components/schemas/UploadedArchiveRootPublicationOut |  |
| `failure` | yes | null |  |
| `last_uploaded_at` | yes | string |  |
| `last_verified_at` | yes | string |  |
| `object_count` | yes | integer |  |
| `state` | yes | string |  |
| `storage_prefix` | yes | string |  |
| `store` | yes | #/components/schemas/ArchiveStoreName |  |
| `stored_bytes` | yes | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 92b812376a7370b1f76bf65d5ed9af571721497a1ad4dd6df0c3fee0eb5968f0 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "archive_root": {
      "$ref": "#/components/schemas/UploadedArchiveRootPublicationOut"
    },
    "failure": {
      "title": "Failure",
      "type": "null"
    },
    "last_uploaded_at": {
      "title": "Last Uploaded At",
      "type": "string"
    },
    "last_verified_at": {
      "title": "Last Verified At",
      "type": "string"
    },
    "object_count": {
      "minimum": 1,
      "title": "Object Count",
      "type": "integer"
    },
    "state": {
      "const": "uploaded",
      "title": "State",
      "type": "string"
    },
    "storage_prefix": {
      "minLength": 1,
      "title": "Storage Prefix",
      "type": "string"
    },
    "store": {
      "$ref": "#/components/schemas/ArchiveStoreName"
    },
    "stored_bytes": {
      "minimum": 1,
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
  "title": "UploadedArchiveCopyOut",
  "type": "object"
}
```
