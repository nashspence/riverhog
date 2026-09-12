# GET /v1/retrieval-cache

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:get-v1-retrieval-cache:452a9fd7bc -->

Retrieval Cache Status

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [retrieval-cache](families/retrieval-cache/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-37955c5a9e"></a>
- <a id="s-90de4c1b66"></a>`operationId`: retrieval_cache_status
- <a id="s-93796d4f84"></a>`summary`: Retrieval Cache Status
- <a id="s-1f3148a04d"></a>`security`: `[{"HTTPBearer": []}]`

### Responses

| Status | Description |
|---|---|
| <a id="s-27e211ffbb"></a>`200` | Successful Response |
| <a id="s-9233e64928"></a>`400` | Bad Request |
| <a id="s-bad2ef208e"></a>`401` | Unauthorized |
| <a id="s-62c1707d91"></a>`403` | Forbidden |
| <a id="s-7bfbb4ccc4"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: retrieval_cache_status](../operation/operation-parity-retrieval-cache-status.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: RetrievalCacheStatusOut](schemas-retrievalcachestatusout.md)

## Governing policies

- <a id="pa-f3b91d8809"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1retrieval-cache/get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a718c04d168b43ef6e0246168e683492424546fb885a821df244af2264e0612e -->

```json
{
  "operationId": "retrieval_cache_status",
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/RetrievalCacheStatusOut"
          }
        }
      },
      "description": "Successful Response"
    },
    "400": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Bad Request",
      "x-riverhog-error-codes": [
        "bad_request"
      ]
    },
    "401": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Unauthorized",
      "x-riverhog-error-codes": [
        "unauthorized"
      ]
    },
    "403": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Forbidden",
      "x-riverhog-error-codes": [
        "forbidden"
      ]
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
  "security": [
    {
      "HTTPBearer": []
    }
  ],
  "summary": "Retrieval Cache Status",
  "tags": [
    "retrieval"
  ],
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "catalog:read"
      ]
    }
  ]
}
```
