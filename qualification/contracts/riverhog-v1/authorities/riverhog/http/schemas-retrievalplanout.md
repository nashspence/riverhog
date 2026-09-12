# schemas: RetrievalPlanOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalplanout:d94f1ee78a -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalPlanOut`

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
| value | schema-value | `contract_max` | maximum=10000, minimum=1, reason=schema-maximum |

## Contract

- `title`: RetrievalPlanOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `created_at` | yes | string |  |
| `etag` | yes | object (2 fields) |  |
| `expires_at` | yes | string |  |
| `failure` | yes | object (2 fields) |  |
| `file_count` | yes | integer |  |
| `format` | yes | string |  |
| `id` | yes | string |  |
| `lease_seconds` | yes | integer |  |
| `ready_at` | yes | object (2 fields) |  |
| `requires_restore` | yes | boolean |  |
| `restore_policy` | yes | string |  |
| `state` | yes | string |  |
