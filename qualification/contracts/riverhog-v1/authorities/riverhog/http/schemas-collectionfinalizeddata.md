# schemas: CollectionFinalizedData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionfinalizeddata:13b8d55354 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 4 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionFinalizedData`

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
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `contract_max` | maximum=64, minimum=1, reason=schema-maximum |
| encoded-size | bytes | `contract_max` | maximum=4096, reason=bounded-lifecycle-event-context |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract

- `title`: CollectionFinalizedData
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `actor` | yes | #/components/schemas/RiverhogActor |  |
| `archive_root_sha256` | yes | string |  |
| `bytes_total` | yes | integer |  |
| `cause` | no | object (1 fields) |  |
| `collection_created_at` | yes | string |  |
| `collection_id` | yes | #/components/schemas/CollectionId |  |
| `context` | no | object (2 fields) |  |
| `files_total` | yes | integer |  |
| `initiator` | yes | #/components/schemas/RiverhogActor |  |
