# GET /v1/collection-processing-claims

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:get-v1-collection-processing-claims:33f4c98699 -->

List Processing Claims

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [collection-processing-claims](families/collection-processing-claims/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-c020b12657"></a>
- <a id="s-7e81e1c32e"></a>`operationId`: list_processing_claims
- <a id="s-e2fd121d73"></a>`summary`: List Processing Claims
- <a id="s-b7154c2973"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-2321207667"></a>`page_size` | query | no | type="integer"; minimum=1; maximum=100 |
| <a id="s-37f200ec6c"></a>`page_token` | query | no | anyOf=#/components/schemas/BrowsePageToken \| type="null" |
| <a id="s-96a5f47301"></a>`state` | query | no | anyOf=type="string"; enum=["active","settled","retiring","abandoned","released"] \| type="null" |
| <a id="s-6308dc537d"></a>`sort` | query | no | type="string"; enum=["created_at","updated_at","expires_at","state","work_id","execution_id"] |
| <a id="s-fa342e9615"></a>`order` | query | no | type="string"; enum=["asc","desc"] |

### Responses

| Status | Description |
|---|---|
| <a id="s-c7dbec4dcf"></a>`200` | Successful Response |
| <a id="s-2ab72efb00"></a>`400` | Bad Request |
| <a id="s-69b1b770d1"></a>`401` | Unauthorized |
| <a id="s-ff02bb11b9"></a>`403` | Forbidden |
| <a id="s-d4893dfda8"></a>`409` | Conflict |
| <a id="s-911a4536fc"></a>`500` | Internal Server Error |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}; reason="bounded-route-progression"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [GET /v1/collection-processing-claims](#s-c020b12657) | `logical-result-cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-01a11f4824"></a>[parameter page_size](#s-2321207667) | `value · schema-value · contract_max` | shared above |

## Maintained corroboration

### Related interface records

- [Operation parity: list_processing_claims](../operation/operation-parity-list-processing-claims.md)

### Referenced contract dossiers

- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: ProcessingClaimPageDocument](schemas-processingclaimpagedocument.md)

## Governing policies

- <a id="pa-7de69ccc4e"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-a39052c524"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)
- <a id="pa-66fd816f33"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collection-processing-claims/get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 063675e62076a6c949af8ffb546c7f2810184264077ae7ceab970b91a3a040e2 -->

```json
{
  "operationId": "list_processing_claims",
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
      "name": "state",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "enum": [
              "active",
              "settled",
              "retiring",
              "abandoned",
              "released"
            ],
            "type": "string"
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
      "name": "sort",
      "required": false,
      "schema": {
        "default": "updated_at",
        "enum": [
          "created_at",
          "updated_at",
          "expires_at",
          "state",
          "work_id",
          "execution_id"
        ],
        "title": "Sort",
        "type": "string"
      }
    },
    {
      "in": "query",
      "name": "order",
      "required": false,
      "schema": {
        "default": "desc",
        "enum": [
          "asc",
          "desc"
        ],
        "title": "Order",
        "type": "string"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ProcessingClaimPageDocument"
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
        "invalid_state"
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
  "summary": "List Processing Claims",
  "tags": [
    "collection-workflows"
  ],
  "x-riverhog-interface": "client-only-primitive",
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "collection-transforms:control"
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
