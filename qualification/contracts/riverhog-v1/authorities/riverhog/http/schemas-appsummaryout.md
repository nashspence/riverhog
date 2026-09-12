# schemas: AppSummaryOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-appsummaryout:d221fc0283 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/AppSummaryOut`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract

- `title`: AppSummaryOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `active_keys` | yes | integer |  |
| `keys` | yes | integer |  |
| `last_used_at` | yes | object (2 fields) |  |
| `name` | yes | #/components/schemas/ApplicationName |  |
