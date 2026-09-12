# schemas: RetrievalCacheObjectListFiltersOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalcacheobjectlistfiltersout:2fde567a9a -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalCacheObjectListFiltersOut`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract

- `title`: RetrievalCacheObjectListFiltersOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `cache_store` | yes | object (1 fields) |  |
| `collection_id` | yes | object (1 fields) |  |
| `expires_after` | yes | object (2 fields) |  |
| `expires_before` | yes | object (2 fields) |  |
| `protection` | yes | object (1 fields) |  |
| `source_store` | yes | object (1 fields) |  |
| `state` | yes | object (1 fields) |  |
