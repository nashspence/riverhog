# schemas: CollectionUploadSessionFilesRegistrationOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadsessionfilesregistrationout:ebe60d1b35 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadSessionFilesRegistrationOut`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ArchiveStoreName](schemas-archivestorename.md)
- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: CollectionUploadFileOut](schemas-collectionuploadfileout.md)
- [schemas: CollectionUploadVolumeSummaryDocument](schemas-collectionuploadvolumesummarydocument.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract summary

- `title`: CollectionUploadSessionFilesRegistrationOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `archive_store` | yes | #/components/schemas/ArchiveStoreName |  |
| `collection_id` | yes | #/components/schemas/CollectionId |  |
| `encryption_format` | yes | string |  |
| `files` | yes | array |  |
| `ingest_source` | yes | object (2 fields) |  |
| `passphrase_id` | yes | string |  |
| `state` | yes | string |  |
| `volumes` | yes | array |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6ea268b0d0e5b2ad156f8df332d294470f81bb8928a6ad38a70ea7ba067f2f07 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "archive_store": {
      "$ref": "#/components/schemas/ArchiveStoreName"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "encryption_format": {
      "title": "Encryption Format",
      "type": "string"
    },
    "files": {
      "items": {
        "$ref": "#/components/schemas/CollectionUploadFileOut"
      },
      "title": "Files",
      "type": "array"
    },
    "ingest_source": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Ingest Source"
    },
    "passphrase_id": {
      "pattern": "^[A-Za-z0-9_-]{16,128}$",
      "title": "Passphrase Id",
      "type": "string"
    },
    "state": {
      "const": "open",
      "title": "State",
      "type": "string"
    },
    "volumes": {
      "items": {
        "$ref": "#/components/schemas/CollectionUploadVolumeSummaryDocument"
      },
      "title": "Volumes",
      "type": "array"
    }
  },
  "required": [
    "collection_id",
    "ingest_source",
    "archive_store",
    "encryption_format",
    "passphrase_id",
    "state",
    "files",
    "volumes"
  ],
  "title": "CollectionUploadSessionFilesRegistrationOut",
  "type": "object"
}
```
