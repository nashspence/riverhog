# GET /v1/retrieval-cache/objects

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:get-v1-retrieval-cache-objects:91e5040e4e -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `retrieval-cache` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1retrieval-cache~1objects/get`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/route-progression/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Related interface records

- [Operation parity: list_retrieval_cache_objects](../operation/operation-parity-list-retrieval-cache-objects.md)

## Referenced contract dossiers

- [schemas: ArchiveStoreName](schemas-archivestorename.md)
- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
- [schemas: BrowseQuery](schemas-browsequery.md)
- [schemas: CollectionIdParameter](schemas-collectionidparameter.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: RetrievalCacheObjectListOut](schemas-retrievalcacheobjectlistout.md)
- [schemas: RetrievalCacheProtection](schemas-retrievalcacheprotection.md)
- [schemas: RetrievalCacheSort](schemas-retrievalcachesort.md)
- [schemas: RetrievalCacheState](schemas-retrievalcachestate.md)
- [schemas: RetrievalCacheStoreName](schemas-retrievalcachestorename.md)
- [schemas: SortOrder](schemas-sortorder.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| logical-result-cardinality | items | `segmented_no_total_max` | reason=bounded-route-progression |
| value | schema-value | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |

## Contract summary

- `operationId`: list_retrieval_cache_objects
- `summary`: List Retrieval Cache Objects
- `security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| `page_size` | query | no | integer |
| `page_token` | query | no | object (2 fields) |
| `q` | query | no | object (2 fields) |
| `collection_id` | query | no | object (2 fields) |
| `source_store` | query | no | object (2 fields) |
| `cache_store` | query | no | object (2 fields) |
| `state` | query | no | object (2 fields) |
| `protection` | query | no | object (2 fields) |
| `expires_before` | query | no | object (2 fields) |
| `expires_after` | query | no | object (2 fields) |
| `sort` | query | no | #/components/schemas/RetrievalCacheSort |
| `order` | query | no | #/components/schemas/SortOrder |

### Responses

| Status | Description |
|---|---|
| `200` | Successful Response |
| `400` | Bad Request |
| `401` | Unauthorized |
| `403` | Forbidden |
| `500` | Internal Server Error |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 02c7eb4735c4b8d8d9333ff27032238a27390a843c794a60d4d4647cd399f0a9 -->

```json
{
  "operationId": "list_retrieval_cache_objects",
  "parameters": [
    {
      "in": "query",
      "name": "page_size",
      "required": false,
      "schema": {
        "default": 25,
        "maximum": 100,
        "minimum": 1,
        "title": "Page Size",
        "type": "integer"
      }
    },
    {
      "in": "query",
      "name": "page_token",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "$ref": "#/components/schemas/BrowsePageToken"
          },
          {
            "type": "null"
          }
        ],
        "title": "Page Token"
      }
    },
    {
      "in": "query",
      "name": "q",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "$ref": "#/components/schemas/BrowseQuery"
          },
          {
            "type": "null"
          }
        ],
        "title": "Q"
      }
    },
    {
      "in": "query",
      "name": "collection_id",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "$ref": "#/components/schemas/CollectionIdParameter"
          },
          {
            "type": "null"
          }
        ],
        "title": "Collection Id"
      }
    },
    {
      "in": "query",
      "name": "source_store",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "$ref": "#/components/schemas/ArchiveStoreName"
          },
          {
            "type": "null"
          }
        ],
        "title": "Source Store"
      }
    },
    {
      "in": "query",
      "name": "cache_store",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "$ref": "#/components/schemas/RetrievalCacheStoreName"
          },
          {
            "type": "null"
          }
        ],
        "title": "Cache Store"
      }
    },
    {
      "in": "query",
      "name": "state",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "$ref": "#/components/schemas/RetrievalCacheState"
          },
          {
            "type": "null"
          }
        ],
        "title": "State"
      }
    },
    {
      "in": "query",
      "name": "protection",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "$ref": "#/components/schemas/RetrievalCacheProtection"
          },
          {
            "type": "null"
          }
        ],
        "title": "Protection"
      }
    },
    {
      "in": "query",
      "name": "expires_before",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "type": "string"
          },
          {
            "type": "null"
          }
        ],
        "title": "Expires Before"
      }
    },
    {
      "in": "query",
      "name": "expires_after",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "type": "string"
          },
          {
            "type": "null"
          }
        ],
        "title": "Expires After"
      }
    },
    {
      "in": "query",
      "name": "sort",
      "required": false,
      "schema": {
        "$ref": "#/components/schemas/RetrievalCacheSort",
        "default": "cached_at"
      }
    },
    {
      "in": "query",
      "name": "order",
      "required": false,
      "schema": {
        "$ref": "#/components/schemas/SortOrder",
        "default": "desc"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/RetrievalCacheObjectListOut"
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
  "summary": "List Retrieval Cache Objects",
  "tags": [
    "retrieval"
  ],
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "catalog:read"
      ]
    }
  ],
  "x-riverhog-read-collection": {
    "default_page_size": 25,
    "kind": "mutable-browse",
    "maximum_page_size": 100,
    "next_page_token_field": "next_page_token",
    "page_size_parameter": "page_size",
    "page_token_parameter": "page_token"
  }
}
```
