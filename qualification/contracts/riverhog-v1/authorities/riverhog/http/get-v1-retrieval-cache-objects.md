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

<a id="s-8d492ab67520"></a>
- <a id="s-b7fc6d300747"></a>`operationId`: list_retrieval_cache_objects
- <a id="s-88b74e3e7d0b"></a>`summary`: List Retrieval Cache Objects
- <a id="s-8df797da7874"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-e92cbb3fea92"></a>`page_size` | query | no | type="integer"; minimum=1; maximum=100 |
| <a id="s-dd0f2add9745"></a>`page_token` | query | no | anyOf=#/components/schemas/BrowsePageToken \| type="null" |
| <a id="s-aded9a430cd0"></a>`q` | query | no | anyOf=#/components/schemas/BrowseQuery \| type="null" |
| <a id="s-c8add90ad778"></a>`collection_id` | query | no | anyOf=#/components/schemas/CollectionIdParameter \| type="null" |
| <a id="s-a9e9c9b38989"></a>`source_store` | query | no | anyOf=#/components/schemas/ArchiveStoreName \| type="null" |
| <a id="s-1f2a183ae900"></a>`cache_store` | query | no | anyOf=#/components/schemas/RetrievalCacheStoreName \| type="null" |
| <a id="s-278a2451e855"></a>`state` | query | no | anyOf=#/components/schemas/RetrievalCacheState \| type="null" |
| <a id="s-2cdb46627af1"></a>`protection` | query | no | anyOf=#/components/schemas/RetrievalCacheProtection \| type="null" |
| <a id="s-09dfc97256e4"></a>`expires_before` | query | no | anyOf=type="string" \| type="null" |
| <a id="s-bcbff18e3a69"></a>`expires_after` | query | no | anyOf=type="string" \| type="null" |
| <a id="s-3f24af8c90d0"></a>`sort` | query | no | $ref="#/components/schemas/RetrievalCacheSort" |
| <a id="s-2e8818cb8a62"></a>`order` | query | no | $ref="#/components/schemas/SortOrder" |

### Responses

| Status | Description |
|---|---|
| <a id="s-5f7e7e3a7a2b"></a>`200` | Successful Response |
| <a id="s-b3e35d547b69"></a>`400` | Bad Request |
| <a id="s-c9c2c99357ec"></a>`401` | Unauthorized |
| <a id="s-e4b39fdf4e44"></a>`403` | Forbidden |
| <a id="s-53f3f02986f7"></a>`500` | Internal Server Error |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb21)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}; reason="bounded-route-progression"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [GET /v1/retrieval-cache/objects](#s-8d492ab67520) | `logical-result-cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-263795c07990"></a>parameter page_size | `value · schema-value · contract_max` | shared above |

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

- <a id="pa-1a93151949d9"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-68660773dfd9"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb21)
- <a id="pa-eb368b2f7aff"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
