# schemas: ListProvenanceJournalAgentsResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-listprovenancejournalagentsresponse:4a3def0a49 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ListProvenanceJournalAgentsResponse`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/route-progression/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | reason=bounded-route-page |
| value | schema-value | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |

## Contract

- `title`: ListProvenanceJournalAgentsResponse
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `agents` | yes | array |  |
| `collection_id` | yes | #/components/schemas/CollectionId |  |
| `journal_id` | yes | #/components/schemas/ProvenanceJournalId |  |
| `next_page_token` | yes | object (1 fields) |  |
| `page_size` | yes | integer |  |
