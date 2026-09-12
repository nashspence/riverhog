# GET /v1/retrieval-cache/objects

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:get-v1-retrieval-cache-objects:91e5040e4e -->

List Retrieval Cache Objects

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [retrieval-cache](families/retrieval-cache/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-8d492ab675"></a>
- <a id="s-b7fc6d3007"></a>`operationId`: list_retrieval_cache_objects
- <a id="s-88b74e3e7d"></a>`summary`: List Retrieval Cache Objects
- <a id="s-8df797da78"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-e92cbb3fea"></a>`page_size` | query | no | type="integer"; minimum=1; maximum=100 |
| <a id="s-dd0f2add97"></a>`page_token` | query | no | anyOf=#/components/schemas/BrowsePageToken \| type="null" |
| <a id="s-aded9a430c"></a>`q` | query | no | anyOf=#/components/schemas/BrowseQuery \| type="null" |
| <a id="s-c8add90ad7"></a>`collection_id` | query | no | anyOf=#/components/schemas/CollectionIdParameter \| type="null" |
| <a id="s-a9e9c9b389"></a>`source_store` | query | no | anyOf=#/components/schemas/ArchiveStoreName \| type="null" |
| <a id="s-1f2a183ae9"></a>`cache_store` | query | no | anyOf=#/components/schemas/RetrievalCacheStoreName \| type="null" |
| <a id="s-278a2451e8"></a>`state` | query | no | anyOf=#/components/schemas/RetrievalCacheState \| type="null" |
| <a id="s-2cdb46627a"></a>`protection` | query | no | anyOf=#/components/schemas/RetrievalCacheProtection \| type="null" |
| <a id="s-09dfc97256"></a>`expires_before` | query | no | anyOf=type="string" \| type="null" |
| <a id="s-bcbff18e3a"></a>`expires_after` | query | no | anyOf=type="string" \| type="null" |
| <a id="s-3f24af8c90"></a>`sort` | query | no | $ref="#/components/schemas/RetrievalCacheSort" |
| <a id="s-2e8818cb8a"></a>`order` | query | no | $ref="#/components/schemas/SortOrder" |

### Responses

| Status | Description |
|---|---|
| <a id="s-5f7e7e3a7a"></a>`200` | Successful Response |
| <a id="s-b3e35d547b"></a>`400` | Bad Request |
| <a id="s-c9c2c99357"></a>`401` | Unauthorized |
| <a id="s-e4b39fdf4e"></a>`403` | Forbidden |
| <a id="s-53f3f02986"></a>`500` | Internal Server Error |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}; reason="bounded-route-progression"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [GET /v1/retrieval-cache/objects](#s-8d492ab675) | `logical-result-cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-263795c079"></a>[parameter page_size](#s-e92cbb3fea) | `value · schema-value · contract_max` | shared above |

## Maintained corroboration

### Related interface records

- [Operation parity: list_retrieval_cache_objects](../operation/operation-parity-list-retrieval-cache-objects.md)

### Referenced contract dossiers

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

## Governing policies

- <a id="pa-1a93151949"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-68660773df"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)
- <a id="pa-eb368b2f7a"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1retrieval-cache~1objects/get`

### Exact owned JSON

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
