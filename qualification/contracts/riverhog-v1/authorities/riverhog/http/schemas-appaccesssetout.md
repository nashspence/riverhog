# schemas: AppAccessSetOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-appaccesssetout:f7577e9305 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/AppAccessSetOut`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract

- `title`: AppAccessSetOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `access` | yes | #/components/schemas/ApplicationAccessGrantSet |  |
| `app` | yes | #/components/schemas/ApplicationName |  |
| `key_id` | yes | #/components/schemas/ApplicationKeyId |  |
