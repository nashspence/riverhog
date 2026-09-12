# schemas: ArchiveDownloadAllowanceOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-archivedownloadallowanceout:7189efe81a -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveDownloadAllowanceOut`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract

- `title`: ArchiveDownloadAllowanceOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `accounted_bytes` | yes | integer |  |
| `allowance_bytes` | yes | integer |  |
| `effective_limit_bytes` | yes | integer |  |
| `month_started_at` | yes | string |  |
| `remaining_bytes` | yes | integer |  |
| `reserved_bytes` | yes | integer |  |
| `resets_at` | yes | string |  |
| `safety_buffer_bytes` | yes | integer |  |
| `state` | yes | string |  |
| `store` | yes | #/components/schemas/ArchiveStoreName |  |
