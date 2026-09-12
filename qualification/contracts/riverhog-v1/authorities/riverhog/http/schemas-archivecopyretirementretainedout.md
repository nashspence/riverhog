# schemas: ArchiveCopyRetirementRetainedOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-archivecopyretirementretainedout:a78d4d3816 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyRetirementRetainedOut`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract

- `title`: ArchiveCopyRetirementRetainedOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `last_verified_at` | yes | string |  |
| `remote_storage_bytes` | yes | integer |  |
| `store` | yes | #/components/schemas/ArchiveStoreName |  |
