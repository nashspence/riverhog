# GET /v1/collections/{collection_id}/tags

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:get-v1-collections-collection-id-tags:98913078f9 -->

List Collection Tags

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [collections](families/collections/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

<a id="s-ebc23e6a3b"></a>
- <a id="s-8d52fce6b4"></a>`operationId`: list_collection_tags
- <a id="s-a99fe14e37"></a>`summary`: List Collection Tags
- <a id="s-df0b975dea"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-61d6bf74c3"></a>`collection_id` | path | yes | type="integer"; minimum=1 |
| <a id="s-d879f8ea42"></a>`revision` | query | yes | type="integer"; minimum=1 |
| <a id="s-a651f24cea"></a>`tag_set_identity` | query | yes | type="string"; pattern="^[0-9a-f]{64}$" |
| <a id="s-9d44d0e0b3"></a>`page_size` | query | no | type="integer"; minimum=1; maximum=100 |
| <a id="s-8ca4af851e"></a>`page_token` | query | no | anyOf=#/components/schemas/BrowsePageToken \| type="null" |

### Responses

| Status | Description |
|---|---|
| <a id="s-55b4e0fcfa"></a>`200` | Successful Response |
| <a id="s-91b9e58165"></a>`400` | Bad Request |
| <a id="s-04b4e54794"></a>`401` | Unauthorized |
| <a id="s-177afcce12"></a>`403` | Forbidden |
| <a id="s-60b919b53b"></a>`404` | Not Found |
| <a id="s-1968e4d755"></a>`409` | Conflict |
| <a id="s-7daee174eb"></a>`500` | Internal Server Error |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: progression={"authority":"collection-tag-set","authority_parameter":"tag_set_identity","cursor_parameter":"page_token","kind":"exact-authority-page","limit_parameter":"page_size"}; reason="bounded-route-progression"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [GET /v1/collections/{collection_id}/tags](#s-ebc23e6a3b) | `logical-result-cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-741ef68ae4"></a>[parameter page_size](#s-9d44d0e0b3) | `value · schema-value · contract_max` | maximum=100; minimum=1; reason="schema-maximum" |
| <a id="s-00e27b38e6"></a>[parameter tag_set_identity](#s-a651f24cea) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Related interface records

- [Operation parity: list_collection_tags](../operation/operation-parity-list-collection-tags.md)

### Referenced contract dossiers

- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
- [schemas: CollectionTagListOut](schemas-collectiontaglistout.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)

## Governing policies

- <a id="pa-e21eb41c50"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-7a8dd2653e"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)
- <a id="pa-fedf9cec54"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collections~1{collection_id}~1tags/get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1689028420bff625de6a170c88aa502bcd9a0115f77269d719af9973d06c12ec -->

```json
{
  "operationId": "list_collection_tags",
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
      "in": "query",
      "name": "revision",
      "required": true,
      "schema": {
        "minimum": 1,
        "title": "Revision",
        "type": "integer"
      }
    },
    {
      "in": "query",
      "name": "tag_set_identity",
      "required": true,
      "schema": {
        "pattern": "^[0-9a-f]{64}$",
        "title": "Tag Set Identity",
        "type": "string"
      }
    },
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
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/CollectionTagListOut"
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
  "summary": "List Collection Tags",
  "tags": [
    "collection-tags"
  ],
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "catalog:read"
      ]
    }
  ],
  "x-riverhog-read-collection": {
    "authority": "collection-tag-set",
    "authority_parameter": "tag_set_identity",
    "cursor_parameter": "page_token",
    "kind": "exact-authority-page",
    "limit_parameter": "page_size"
  }
}
```
