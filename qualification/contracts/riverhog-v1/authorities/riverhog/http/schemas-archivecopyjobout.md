# schemas: ArchiveCopyJobOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-archivecopyjobout:4841696744 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyJobOut`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract

- `title`: ArchiveCopyJobOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `collection_id` | yes | #/components/schemas/CollectionId |  |
| `completed_at` | yes | object (2 fields) |  |
| `destination_store` | yes | #/components/schemas/ArchiveStoreName |  |
| `expires_at` | yes | object (2 fields) |  |
| `failure` | yes | object (2 fields) |  |
| `initiated_by_app` | yes | object (1 fields) |  |
| `initiated_by_key_id` | yes | object (1 fields) |  |
| `ready_at` | yes | object (2 fields) |  |
| `requested_at` | yes | object (2 fields) |  |
| `source_store` | yes | object (1 fields) |  |
| `state` | yes | #/components/schemas/ArchiveCopyState |  |
