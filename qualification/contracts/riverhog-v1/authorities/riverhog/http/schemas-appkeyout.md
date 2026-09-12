# schemas: AppKeyOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-appkeyout:41aeadc2ae -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/AppKeyOut`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract

- `title`: AppKeyOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `access` | yes | #/components/schemas/ApplicationAccessGrantSet |  |
| `app` | yes | #/components/schemas/ApplicationName |  |
| `created_at` | yes | string |  |
| `expires_at` | yes | object (2 fields) |  |
| `id` | yes | #/components/schemas/ApplicationKeyId |  |
| `last_used_at` | yes | object (2 fields) |  |
| `monthly_download_quota_bytes` | yes | object (1 fields) |  |
| `revoked_at` | yes | object (2 fields) |  |
| `status` | yes | string |  |
