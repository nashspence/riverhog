# schemas: ArchiveCopyRequestedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-archivecopyrequestedevent:633035295d -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyRequestedEvent`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract

- `title`: ArchiveCopyRequestedEvent
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `data` | yes | #/components/schemas/ArchiveCopyRequestedData |  |
| `datacontenttype` | no | string |  |
| `id` | yes | string |  |
| `source` | yes | string |  |
| `specversion` | no | string |  |
| `subject` | no | object (2 fields) |  |
| `time` | yes | string |  |
| `type` | yes | string |  |
