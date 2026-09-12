# GET /health/live

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:get-health-live:8fa3b5bf0d -->

Health Live

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [health](families/health/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-fccde469e0"></a>
- <a id="s-b9f96c021d"></a>`operationId`: health_live
- <a id="s-56efa9f79c"></a>`summary`: Health Live

### Responses

| Status | Description |
|---|---|
| <a id="s-f426d91d27"></a>`200` | Successful Response |
| <a id="s-76649245bd"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: health_live](../operation/operation-parity-health-live.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: HealthResponse](schemas-healthresponse.md)

## Governing policies

- <a id="pa-3e9d9e669f"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1health~1live/get`

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
