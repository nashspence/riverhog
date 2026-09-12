# schemas: KeyDownloadQuotaOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-keydownloadquotaout:eccde43198 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/KeyDownloadQuotaOut`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract

- `title`: KeyDownloadQuotaOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `accounted_bytes` | yes | integer |  |
| `app` | yes | #/components/schemas/ApplicationName |  |
| `id` | yes | string |  |
| `key_id` | yes | #/components/schemas/ApplicationKeyId |  |
| `key_status` | yes | string |  |
| `month_started_at` | yes | string |  |
| `monthly_bytes` | yes | object (1 fields) |  |
| `remaining_bytes` | yes | object (2 fields) |  |
| `reserved_bytes` | yes | integer |  |
| `resets_at` | yes | string |  |
