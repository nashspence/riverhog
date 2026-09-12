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

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract

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
