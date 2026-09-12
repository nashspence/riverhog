# schemas: RetrievalCacheObjectOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalcacheobjectout:2d8f767919 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalCacheObjectOut`

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
- [schemas: RetrievalCacheState](schemas-retrievalcachestate.md)
- [schemas: RetrievalCacheStoreName](schemas-retrievalcachestorename.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract summary

- `title`: RetrievalCacheObjectOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `cache_store` | yes | #/components/schemas/RetrievalCacheStoreName |  |
| `cached_at` | yes | string |  |
| `collection_id` | yes | #/components/schemas/CollectionId |  |
| `lease_categories` | yes | array |  |
| `new_archive_expires_at` | yes | object (2 fields) |  |
| `object_id` | yes | string |  |
| `protected_until` | yes | object (2 fields) |  |
| `retrieval_job_leases` | yes | integer |  |
| `source_store` | yes | #/components/schemas/ArchiveStoreName |  |
| `state` | yes | #/components/schemas/RetrievalCacheState |  |
| `stored_bytes` | yes | integer |  |
| `stored_sha256` | yes | object (2 fields) |  |
| `verified_at` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9cffc6baaebb7190c36f83db762b17e8a4892c78028fb285265bfe08e054a3e4 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "cache_store": {
      "$ref": "#/components/schemas/RetrievalCacheStoreName"
    },
    "cached_at": {
      "title": "Cached At",
      "type": "string"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "lease_categories": {
      "items": {
        "enum": [
          "new_archive",
          "retrieval_job"
        ],
        "type": "string"
      },
      "title": "Lease Categories",
      "type": "array"
    },
    "new_archive_expires_at": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "New Archive Expires At"
    },
    "object_id": {
      "title": "Object Id",
      "type": "string"
    },
    "protected_until": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Protected Until"
    },
    "retrieval_job_leases": {
      "title": "Retrieval Job Leases",
      "type": "integer"
    },
    "source_store": {
      "$ref": "#/components/schemas/ArchiveStoreName"
    },
    "state": {
      "$ref": "#/components/schemas/RetrievalCacheState"
    },
    "stored_bytes": {
      "title": "Stored Bytes",
      "type": "integer"
    },
    "stored_sha256": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Stored Sha256"
    },
    "verified_at": {
      "title": "Verified At",
      "type": "string"
    }
  },
  "required": [
    "collection_id",
    "source_store",
    "cache_store",
    "object_id",
    "state",
    "stored_bytes",
    "stored_sha256",
    "cached_at",
    "verified_at",
    "protected_until",
    "new_archive_expires_at",
    "lease_categories",
    "retrieval_job_leases"
  ],
  "title": "RetrievalCacheObjectOut",
  "type": "object"
}
```
