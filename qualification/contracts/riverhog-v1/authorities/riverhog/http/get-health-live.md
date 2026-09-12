# GET /health/live

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:get-health-live:cab0be7583 -->

Health Live

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [health](families/health/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-ff881e1b82"></a>
- <a id="s-6e8619966e"></a>`operationId`: health_live
- <a id="s-931d9426c9"></a>`summary`: Health Live

### Responses

| Status | Description |
|---|---|
| <a id="s-984409c5f2"></a>`200` | Successful Response |
| <a id="s-2d1085b2a7"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: health_live](../operation/operation-parity-health-live.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: HealthResponse](schemas-healthresponse.md)

## Governing policies

- <a id="pa-b2967a97f3"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1health~1live/get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cc5ca13fd3c259e2a7f72bf552be42bd70202efd06ac4ceac673e8899edb356c -->

```json
{
  "operationId": "health_live",
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
    },
    "500": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Internal Server Error",
      "x-riverhog-error-codes": [
        "internal_error"
      ]
    }
  },
  "summary": "Health Live",
  "tags": [
    "health"
  ]
}
```
