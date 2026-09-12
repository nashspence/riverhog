# schemas: ArchiveCopyIssueData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-archivecopyissuedata:30bac30d66 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 4 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyIssueData`

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
| encoded-size | bytes | `contract_max` | maximum=4096, reason=bounded-lifecycle-event-context |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `contract_max` | maximum=16384, minimum=1, reason=schema-maximum |

## Contract

- `title`: ArchiveCopyIssueData
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `actor` | yes | #/components/schemas/RiverhogActor |  |
| `cause` | no | object (1 fields) |  |
| `collection_created_at` | yes | string |  |
| `collection_id` | yes | #/components/schemas/CollectionId |  |
| `context` | no | object (2 fields) |  |
| `destination_store` | yes | #/components/schemas/ArchiveStoreName |  |
| `error` | yes | string |  |
| `initiator` | yes | #/components/schemas/RiverhogActor |  |
| `source_store` | yes | #/components/schemas/ArchiveStoreName |  |
| `state` | yes | string |  |
