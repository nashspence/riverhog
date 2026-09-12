# schemas: RetrievalCacheStoreStatusOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalcachestorestatusout:593a25b981 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalCacheStoreStatusOut`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract

- `title`: RetrievalCacheStoreStatusOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `admission_budget_bytes` | no | object (2 fields) |  |
| `admission_enabled` | yes | boolean |  |
| `cache_store` | yes | #/components/schemas/RetrievalCacheStoreName |  |
| `committed_bytes` | yes | integer |  |
| `priority` | yes | integer |  |
| `reserved_bytes` | yes | integer |  |
