# schemas: ArchiveCopyRetirementTargetOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-archivecopyretirementtargetout:2a5a28b673 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyRetirementTargetOut`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract

- `title`: ArchiveCopyRetirementTargetOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `last_verified_at` | yes | string |  |
| `object_count` | yes | integer |  |
| `remote_storage_bytes` | yes | integer |  |
| `store` | yes | #/components/schemas/ArchiveStoreName |  |
