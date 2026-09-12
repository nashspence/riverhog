# schemas: RetrievalFailedData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalfaileddata:8f7bd8dbf6 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 6 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalFailedData`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `contract_max` | maximum=64, minimum=1, reason=schema-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| encoded-size | bytes | `contract_max` | maximum=4096, reason=bounded-lifecycle-event-context |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `contract_max` | maximum=16384, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=300, minimum=1, reason=schema-maximum |

## Contract

- `title`: RetrievalFailedData
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `actor` | yes | #/components/schemas/RiverhogActor |  |
| `cause` | no | object (1 fields) |  |
| `collection_created_at` | no | object (2 fields) |  |
| `collection_id` | no | object (1 fields) |  |
| `collection_ids` | yes | array |  |
| `context` | no | object (2 fields) |  |
| `error` | yes | string |  |
| `initiator` | yes | #/components/schemas/RiverhogActor |  |
| `retrieval_id` | yes | string |  |
| `state` | yes | string |  |
