# POST /v1/retrieval-plans

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:post-v1-retrieval-plans:48aeba5daa -->

Plan Retrieval

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [retrieval-plans](families/retrieval-plans/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-fbaa52409a"></a>
- <a id="s-bc96ce70c6"></a>`operationId`: plan_retrieval
- <a id="s-3310619a06"></a>`summary`: Plan Retrieval
- <a id="s-391fc6e75d"></a>`security`: `[{"HTTPBearer": []}]`

### <a id="s-65a14e3f06"></a>Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/RetrievalPlanRequest"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| <a id="s-72c3dd68a9"></a>`200` | Successful Response |
| <a id="s-795211d328"></a>`400` | Bad Request |
| <a id="s-2d1701ccdb"></a>`401` | Unauthorized |
| <a id="s-31239ebfe3"></a>`403` | Forbidden |
| <a id="s-1356852f5c"></a>`404` | Not Found |
| <a id="s-69241d45c7"></a>`409` | Conflict |
| <a id="s-8e0d502c67"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: plan_retrieval](../operation/operation-parity-plan-retrieval.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: RetrievalPlanOut](schemas-retrievalplanout.md)
- [schemas: RetrievalPlanRequest](schemas-retrievalplanrequest.md)

## Governing policies

- <a id="pa-c07ca80bed"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1retrieval-plans/post`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cb95edb2cf0afb1faf099f3a03596c53e4a6d543d54733166e4a91e3ca39af33 -->

```json
{
  "operationId": "plan_retrieval",
  "requestBody": {
    "content": {
      "application/json": {
        "schema": {
          "$ref": "#/components/schemas/RetrievalPlanRequest"
        }
      }
    },
    "required": true
  },
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/RetrievalPlanOut"
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
    "404": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Not Found",
      "x-riverhog-error-codes": [
        "not_found"
      ]
    },
    "409": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Conflict",
      "x-riverhog-error-codes": [
        "conflict"
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
  "summary": "Plan Retrieval",
  "tags": [
    "retrieval"
  ],
  "x-riverhog-interface": "client-only-primitive",
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "retrieval:manage"
      ]
    }
  ]
}
```
