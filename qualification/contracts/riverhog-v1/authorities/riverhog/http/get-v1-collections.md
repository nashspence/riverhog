# GET /v1/collections

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:get-v1-collections:cbee80191c -->

List Collections

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [collections](families/collections/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

<a id="s-c0edb146f5"></a>
- <a id="s-46c0cf000f"></a>`operationId`: list_collections
- <a id="s-f96134ae88"></a>`summary`: List Collections
- <a id="s-cd13077717"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-2076d85ce1"></a>`page_size` | query | no | type="integer"; minimum=1; maximum=100 |
| <a id="s-9731e95241"></a>`page_token` | query | no | anyOf=#/components/schemas/BrowsePageToken \| type="null" |
| <a id="s-2bab53adb8"></a>`q` | query | no | anyOf=#/components/schemas/BrowseQuery \| type="null" |
| <a id="s-3495833d03"></a>`sort` | query | no | $ref="#/components/schemas/CollectionSort" |
| <a id="s-fba33675a7"></a>`order` | query | no | $ref="#/components/schemas/SortOrder" |
| <a id="s-a62ca229bb"></a>`encryption_format` | query | no | anyOf=type="string" \| type="null" |
| <a id="s-6516891cb2"></a>`passphrase_id` | query | no | anyOf=type="string" \| type="null" |
| <a id="s-c928d33a4d"></a>`tags` | query | no | anyOf=type="array"; maxItems=100; items=(#/components/schemas/CollectionTag); additional keys=`x-riverhog-extent` \| type="null" |

### Responses

| Status | Description |
|---|---|
| <a id="s-64bfaf8f5a"></a>`200` | Successful Response |
| <a id="s-1a717dd59c"></a>`400` | Bad Request |
| <a id="s-8d982699dd"></a>`401` | Unauthorized |
| <a id="s-31c81ac156"></a>`403` | Forbidden |
| <a id="s-c6eb104848"></a>`500` | Internal Server Error |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}; reason="bounded-route-progression"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [GET /v1/collections](#s-c0edb146f5) | `logical-result-cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-0696f31dde"></a>[parameter page_size](#s-2076d85ce1) | `value · schema-value · contract_max` | minimum=1; reason="schema-maximum" |
| <a id="s-adcb77fe2d"></a>[parameter tags · array value](#s-c928d33a4d) | `cardinality · items · contract_max` | reason="bounded-exact-tag-selector-batch" |

## Maintained corroboration

### Related interface records

- [Operation parity: list_collections](../operation/operation-parity-list-collections.md)

### Referenced contract dossiers

- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
- [schemas: BrowseQuery](schemas-browsequery.md)
- [schemas: CollectionSort](schemas-collectionsort.md)
- [schemas: CollectionTag](schemas-collectiontag.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: ListCollectionsResponse](schemas-listcollectionsresponse.md)
- [schemas: SortOrder](schemas-sortorder.md)

## Governing policies

- <a id="pa-162c871a6b"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-a41b741a6e"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)
- <a id="pa-2e5c332df2"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collections/get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1a2704e96ebf680261ded833af9841cca8d37c22826d887688a61564672126f8 -->

```json
{
  "operationId": "list_collections",
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
      "name": "sort",
      "required": false,
      "schema": {
        "$ref": "#/components/schemas/CollectionSort",
        "default": "id"
      }
    },
    {
      "in": "query",
      "name": "order",
      "required": false,
      "schema": {
        "$ref": "#/components/schemas/SortOrder",
        "default": "asc"
      }
    },
    {
      "in": "query",
      "name": "encryption_format",
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
        "title": "Encryption Format"
      }
    },
    {
      "in": "query",
      "name": "passphrase_id",
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
        "title": "Passphrase Id"
      }
    },
    {
      "in": "query",
      "name": "tags",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "items": {
              "$ref": "#/components/schemas/CollectionTag"
            },
            "maxItems": 100,
            "type": "array",
            "x-riverhog-extent": {
              "policy": "contract_max",
              "reason": "bounded-exact-tag-selector-batch"
            }
          },
          {
            "type": "null"
          }
        ],
        "title": "Tags"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ListCollectionsResponse"
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
  "summary": "List Collections",
  "tags": [
    "collections"
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
