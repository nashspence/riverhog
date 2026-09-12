# GET /v1/retrieval-cache/objects/{collection_id}/{source_store}/{object_id}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:get-v1-retrieval-cache-objects-collection-bf09699679:aa97bc0a0c -->

Get Retrieval Cache Object

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [retrieval-cache](families/retrieval-cache/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-9a8ec3392d94"></a>
- <a id="s-ef02a9d664dc"></a>`operationId`: get_retrieval_cache_object
- <a id="s-3da411fdfb18"></a>`summary`: Get Retrieval Cache Object
- <a id="s-1a2535641bb6"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-3f640503ffb9"></a>`collection_id` | path | yes | type="integer"; minimum=1 |
| <a id="s-b5fc534559e9"></a>`source_store` | path | yes | type="string"; pattern="^[a-z0-9]+(?:-[a-z0-9]+)*$" |
| <a id="s-847eae74a706"></a>`object_id` | path | yes | type="string" |

### Responses

| Status | Description |
|---|---|
| <a id="s-ffc0a1dc71f8"></a>`200` | Successful Response |
| <a id="s-dff249924ed1"></a>`400` | Bad Request |
| <a id="s-8a8490528c10"></a>`401` | Unauthorized |
| <a id="s-c757eebef1f9"></a>`403` | Forbidden |
| <a id="s-610fc23789e1"></a>`404` | Not Found |
| <a id="s-948f582d708f"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: get_retrieval_cache_object](../operation/operation-parity-get-retrieval-cache-object.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: RetrievalCacheObjectOut](schemas-retrievalcacheobjectout.md)

## Governing policies

- <a id="pa-2d3d7cb50aa3"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1retrieval-cache~1objects~1{collection_id}~1{source_store}~1{object_id}/get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4c5ed7750767a908eece2b94a833375a8a98444cb5e3b8c3e5dd14d1f9425a9b -->

```json
{
  "operationId": "get_retrieval_cache_object",
  "parameters": [
    {
      "in": "path",
      "name": "collection_id",
      "required": true,
      "schema": {
        "minimum": 1,
        "title": "Collection Id",
        "type": "integer"
      }
    },
    {
      "in": "path",
      "name": "source_store",
      "required": true,
      "schema": {
        "pattern": "^[a-z0-9]+(?:-[a-z0-9]+)*$",
        "title": "Source Store",
        "type": "string"
      }
    },
    {
      "in": "path",
      "name": "object_id",
      "required": true,
      "schema": {
        "title": "Object Id",
        "type": "string"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/RetrievalCacheObjectOut"
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
  "summary": "Get Retrieval Cache Object",
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
