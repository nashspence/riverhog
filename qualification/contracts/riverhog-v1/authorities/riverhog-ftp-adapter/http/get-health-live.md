# GET /health/live

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog-ftp-adapter:get-health-live:6985f2b210 -->

Health Live

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [http](index.md) |
| Family | [health](index.md#f-290093cc876a) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-e0652acfec14"></a>
- <a id="s-9b26680856b1"></a>`operationId`: ftp_adapter_health_live
- <a id="s-b44daa8478ca"></a>`summary`: Health Live

### Responses

| Status | Description |
|---|---|
| <a id="s-ab2271298033"></a>`200` | Successful Response |

## Maintained corroboration

### Related interface records

- [Operation parity: ftp_adapter_health_live](../operation/operation-parity-ftp-adapter-health-live.md)

### Referenced contract dossiers

- [schemas: HealthResponse](schemas-healthresponse.md)

## Governing policies

- <a id="pa-5ab978fa7728"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog-ftp-adapter](../../../evidence/sources.md#src-c3a51ac29ac7) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog-ftp-adapter/paths/~1health~1live/get`

### Exact owned JSON

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
