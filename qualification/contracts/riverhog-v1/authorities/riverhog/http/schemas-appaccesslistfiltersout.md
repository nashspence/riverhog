# schemas: AppAccessListFiltersOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-appaccesslistfiltersout:80c05b15d7 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/AppAccessListFiltersOut`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract

- `title`: AppAccessListFiltersOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `active` | yes | object (2 fields) |  |
| `app` | yes | object (1 fields) |  |
| `key_id` | yes | object (1 fields) |  |
| `permission` | yes | object (1 fields) |  |
| `resource` | yes | object (1 fields) |  |
