# schemas: RetrievalJobOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievaljobout:77f19d3812 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalJobOut`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract

- `title`: RetrievalJobOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `canceled_at` | yes | object (2 fields) |  |
| `completed_at` | yes | object (2 fields) |  |
| `created_at` | yes | string |  |
| `expires_at` | yes | object (2 fields) |  |
| `failure` | yes | object (2 fields) |  |
| `id` | yes | string |  |
| `lease_seconds` | yes | integer |  |
| `plan_etag` | yes | string |  |
| `plan_id` | yes | string |  |
| `ready_at` | yes | object (2 fields) |  |
| `requested_at` | yes | object (2 fields) |  |
| `requires_restore` | yes | boolean |  |
| `restore_policy` | yes | string |  |
| `restore_requested_at` | yes | object (2 fields) |  |
| `state` | yes | string |  |
