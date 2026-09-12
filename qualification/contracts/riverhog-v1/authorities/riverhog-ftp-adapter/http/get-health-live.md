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

## Referenced contract dossiers

- [schemas: HealthResponse](schemas-healthresponse.md)

## Contract summary

- `operationId`: ftp_adapter_health_live
- `summary`: Health Live

### Responses

| Status | Description |
|---|---|
| `200` | Successful Response |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 648c33fcea1d5cfe6b1e4086907c6cf0758fac63142bd540eaced02c5ecdf6a2 -->

```json
{
  "operationId": "ftp_adapter_health_live",
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/HealthResponse"
          }
        }
      },
      "description": "Successful Response"
    }
  },
  "summary": "Health Live",
  "tags": [
    "health"
  ]
}
```
