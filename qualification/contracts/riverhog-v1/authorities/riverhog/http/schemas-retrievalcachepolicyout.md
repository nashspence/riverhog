# schemas: RetrievalCachePolicyOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalcachepolicyout:a4be5f615e -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalCachePolicyOut`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract

- `title`: RetrievalCachePolicyOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `new_archive_lease_seconds` | yes | integer |  |
| `pending_timeout_seconds` | yes | integer |  |
| `restore_poll_interval_seconds` | yes | integer |  |
| `retrieval_default_lease_seconds` | yes | integer |  |
| `retrieval_max_lease_seconds` | yes | integer |  |
| `sweep_interval_seconds` | yes | integer |  |
