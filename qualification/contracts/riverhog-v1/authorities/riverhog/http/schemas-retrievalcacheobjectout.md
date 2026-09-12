# schemas: RetrievalCacheObjectOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalcacheobjectout:2d8f767919 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-d8de960a53b0"></a>
- <a id="s-71b5428fd574"></a>`title`: RetrievalCacheObjectOut
- <a id="s-023b459ad5c6"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-06cc3eb42716"></a>`cache_store` | yes | #/components/schemas/RetrievalCacheStoreName |  |
| <a id="s-3b10f667fb39"></a>`cached_at` | yes | type="string" |  |
| <a id="s-d3807612bf01"></a>`collection_id` | yes | #/components/schemas/CollectionId |  |
| <a id="s-627408e57758"></a>`lease_categories` | yes | type="array"; items=(type="string"; enum=["new_archive","retrieval_job"]) |  |
| <a id="s-5e4b6f164b04"></a>`new_archive_expires_at` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-51bc3513f03e"></a>`object_id` | yes | type="string" |  |
| <a id="s-d92ae721874b"></a>`protected_until` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-47dbf3c6e5aa"></a>`retrieval_job_leases` | yes | type="integer" |  |
| <a id="s-ae87dd0d797f"></a>`source_store` | yes | #/components/schemas/ArchiveStoreName |  |
| <a id="s-cbb7f490d39a"></a>`state` | yes | #/components/schemas/RetrievalCacheState |  |
| <a id="s-c95d53b4cfe8"></a>`stored_bytes` | yes | type="integer" |  |
| <a id="s-a582abba727e"></a>`stored_sha256` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-e2697d058e50"></a>`verified_at` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field lease_categories](#s-627408e57758) | `cardinality · items · operational_policy` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArchiveStoreName](schemas-archivestorename.md)
- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: RetrievalCacheState](schemas-retrievalcachestate.md)
- [schemas: RetrievalCacheStoreName](schemas-retrievalcachestorename.md)

## Governing policies

- <a id="pa-215df9a10950"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-9e9024e808ba"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalCacheObjectOut`

### Exact owned JSON

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
