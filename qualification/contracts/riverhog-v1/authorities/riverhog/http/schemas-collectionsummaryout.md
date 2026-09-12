# schemas: CollectionSummaryOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionsummaryout:7097828626 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 8 |

## External contract

- `title`: CollectionSummaryOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `archive_copy_count` | yes | type="integer"; minimum=0 |  |
| `archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `bytes` | yes | type="integer" |  |
| `content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `created_at` | yes | type="string" |  |
| `description` | yes | anyOf=#/components/schemas/CollectionDescription \| type="null" |  |
| `description_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `description_publication` | yes | type="string"; enum=["not_required","current","reconciling"] |  |
| `description_revision` | yes | type="integer"; minimum=0; maximum=9007199254740991 |  |
| `encryption_format` | yes | type="string" |  |
| `files` | yes | type="integer" |  |
| `id` | yes | #/components/schemas/CollectionId |  |
| `passphrase_id` | yes | type="string"; pattern="^[A-Za-z0-9_-]{16,128}$" |  |
| `remote_storage_bytes` | yes | type="integer" |  |
| `tag_publication` | yes | type="string"; enum=["current","reconciling"] |  |
| `tag_revision` | yes | type="integer"; minimum=1; maximum=9007199254740991 |  |
| `tag_set_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| value | schema-value | `contract_max` | maximum=9007199254740991, minimum=0, reason=schema-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `contract_max` | maximum=9007199254740991, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionDescription](schemas-collectiondescription.md)
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

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionSummaryOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4cd5a125d37eac748c8a3bb9e6b711adaefc92edcc8b19d4485cdff4b505e975 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "archive_copy_count": {
      "minimum": 0,
      "title": "Archive Copy Count",
      "type": "integer"
    },
    "archive_root_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Archive Root Sha256",
      "type": "string"
    },
    "bytes": {
      "title": "Bytes",
      "type": "integer"
    },
    "content_identity": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Content Identity",
      "type": "string"
    },
    "created_at": {
      "title": "Created At",
      "type": "string"
    },
    "description": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/CollectionDescription"
        },
        {
          "type": "null"
        }
      ]
    },
    "description_identity": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Description Identity",
      "type": "string"
    },
    "description_publication": {
      "enum": [
        "not_required",
        "current",
        "reconciling"
      ],
      "title": "Description Publication",
      "type": "string"
    },
    "description_revision": {
      "maximum": 9007199254740991,
      "minimum": 0,
      "title": "Description Revision",
      "type": "integer"
    },
    "encryption_format": {
      "title": "Encryption Format",
      "type": "string"
    },
    "files": {
      "title": "Files",
      "type": "integer"
    },
    "id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "passphrase_id": {
      "pattern": "^[A-Za-z0-9_-]{16,128}$",
      "title": "Passphrase Id",
      "type": "string"
    },
    "remote_storage_bytes": {
      "title": "Remote Storage Bytes",
      "type": "integer"
    },
    "tag_publication": {
      "enum": [
        "current",
        "reconciling"
      ],
      "title": "Tag Publication",
      "type": "string"
    },
    "tag_revision": {
      "maximum": 9007199254740991,
      "minimum": 1,
      "title": "Tag Revision",
      "type": "integer"
    },
    "tag_set_identity": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Tag Set Identity",
      "type": "string"
    }
  },
  "required": [
    "id",
    "created_at",
    "description",
    "description_revision",
    "description_identity",
    "description_publication",
    "tag_revision",
    "tag_set_identity",
    "tag_publication",
    "content_identity",
    "archive_root_sha256",
    "encryption_format",
    "passphrase_id",
    "files",
    "bytes",
    "remote_storage_bytes",
    "archive_copy_count"
  ],
  "title": "CollectionSummaryOut",
  "type": "object"
}
```
