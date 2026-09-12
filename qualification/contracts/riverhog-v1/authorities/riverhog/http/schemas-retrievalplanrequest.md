# schemas: RetrievalPlanRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalplanrequest:e6b84ec030 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalPlanRequest`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/bounded-segment/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | maximum=10000, minimum=1, reason=bounded-retrieval-work-request |
| length | characters | `contract_max` | maximum=200, minimum=1, reason=schema-maximum |

## Contract

- `title`: RetrievalPlanRequest
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `files` | yes | array |  |
| `idempotency_key` | yes | string |  |
| `lease_seconds` | no | object (2 fields) |  |
| `restore_policy` | no | string |  |
