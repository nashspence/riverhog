# GET /health/live

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog-ftp-adapter:get-health-live:6985f2b210 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-ftp-adapter` |
| Interface | `http` |
| Family | `health` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog-ftp-adapter/paths/~1health~1live/get`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog-ftp-adapter` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Related interface records

- [Operation parity: ftp_adapter_health_live](../operation/operation-parity-ftp-adapter-health-live.md)

## Contract

- `operationId`: ftp_adapter_health_live
- `summary`: Health Live

### Responses

| Status | Description |
|---|---|
| `200` | Successful Response |
