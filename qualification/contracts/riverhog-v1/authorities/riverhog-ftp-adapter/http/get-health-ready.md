# GET /health/ready

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog-ftp-adapter:get-health-ready:f37f044f2f -->

Health Ready

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [http](index.md) |
| Family | [health](index.md#f-290093cc876a) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-46355a8b994c"></a>
- <a id="s-07449a20f62e"></a>`operationId`: ftp_adapter_health_ready
- <a id="s-d03f68c7018a"></a>`summary`: Health Ready

### Responses

| Status | Description |
|---|---|
| <a id="s-6d25b467efab"></a>`200` | Successful Response |
| <a id="s-fb421d46a9e0"></a>`503` | Service Unavailable |

## Maintained corroboration

### Related interface records

- [Operation parity: ftp_adapter_health_ready](../operation/operation-parity-ftp-adapter-health-ready.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: HealthResponse](schemas-healthresponse.md)

## Governing policies

- <a id="pa-4e707ea273ca"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog-ftp-adapter](../../../evidence/sources.md#src-c3a51ac29ac7) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog-ftp-adapter/paths/~1health~1ready/get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 29fd47a83019e36c295edfb3f7fddd8951196a3feaf23f6ce34cc043219d4bb9 -->

```json
{
  "operationId": "ftp_adapter_health_ready",
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
    "503": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Service Unavailable"
    }
  },
  "summary": "Health Ready",
  "tags": [
    "health"
  ]
}
```
