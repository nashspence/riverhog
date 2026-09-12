# schemas: RetrievalCacheStatusOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalcachestatusout:5e6012674f -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalCacheStatusOut`

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

- `title`: RetrievalCacheStatusOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `configured` | yes | boolean |  |
| `new_archive_enabled` | yes | boolean |  |
| `objects` | yes | integer |  |
| `policy` | yes | #/components/schemas/RetrievalCachePolicyOut |  |
| `protected_objects` | yes | integer |  |
| `stored_bytes` | yes | integer |  |
| `stores` | yes | array |  |
| `unleased_objects` | yes | integer |  |
