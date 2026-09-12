# POST /v1/work/{work_id}/retry

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:post-v1-work-work-id-retry:584f156b48 -->

Retry Work

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [work](families/work/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-f224f3a70b"></a>
- <a id="s-fa5077f709"></a>`operationId`: retry_work
- <a id="s-2e84d3ed9c"></a>`summary`: Retry Work

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-e937ad5d9f"></a>`work_id` | path | yes | type="string" |

### Responses

| Status | Description |
|---|---|
| <a id="s-36498a19d9"></a>`200` | Successful Response |
| <a id="s-1a1d8de0c8"></a>`400` | Bad Request |
| <a id="s-a47633072c"></a>`401` | Unauthorized |
| <a id="s-f26063fcd9"></a>`403` | Forbidden |
| <a id="s-38261a6347"></a>`404` | Not Found |
| <a id="s-b012af4a46"></a>`409` | Conflict |
| <a id="s-afa086344d"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: retry_work](../operation/operation-parity-retry-work.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: WorkView](schemas-workview.md)

## Governing policies

- <a id="pa-6286d1eb56"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1work~1{work_id}~1retry/post`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5ae8f1bcaa90c96ddb2bd36100c1d1e7d7caef1648d3877beb0a5d72d6ae2c05 -->

```json
{
  "operationId": "retry_work",
  "parameters": [
    {
      "in": "path",
      "name": "work_id",
      "required": true,
      "schema": {
        "title": "Work Id",
        "type": "string"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/WorkView"
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
  "summary": "Retry Work",
  "tags": [
    "work"
  ]
}
```
