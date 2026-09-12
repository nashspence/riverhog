# securitySchemes: HTTPBearer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:securityschemes-httpbearer:eed754c0bc -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `securitySchemes` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/securitySchemes/HTTPBearer`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract summary

- `type`: http

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 178204f758fb78d572ed6d4673385f99c0e8697ced9fb775e7bb9851df3e06ee -->

```json
{
  "scheme": "bearer",
  "type": "http"
}
```
