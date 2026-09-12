# GET /v1/app-key-access

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:get-v1-app-key-access:82e1f5f3f3 -->

List App Key Access

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [app-key-access](families/app-key-access/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-7fc99fa765"></a>
- <a id="s-37b8bab0d6"></a>`operationId`: list_app_key_access
- <a id="s-05dbfc097a"></a>`summary`: List App Key Access
- <a id="s-9cbf0f9d52"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-34884d05d9"></a>`page_size` | query | no | type="integer"; minimum=1; maximum=100 |
| <a id="s-0c1b73e009"></a>`page_token` | query | no | anyOf=#/components/schemas/BrowsePageToken \| type="null" |
| <a id="s-9c2c7f379a"></a>`sort` | query | no | $ref="#/components/schemas/ApplicationAccessSort" |
| <a id="s-bb3990152c"></a>`order` | query | no | $ref="#/components/schemas/SortOrder" |
| <a id="s-a8d34e4650"></a>`q` | query | no | anyOf=#/components/schemas/BrowseQuery \| type="null" |
| <a id="s-8c4d8992cd"></a>`app` | query | no | anyOf=#/components/schemas/ApplicationName \| type="null" |
| <a id="s-1db5767afc"></a>`key` | query | no | anyOf=#/components/schemas/ApplicationKeyId \| type="null" |
| <a id="s-a728d4ed13"></a>`permission` | query | no | anyOf=#/components/schemas/ApplicationPermission \| type="null" |
| <a id="s-581adbcf51"></a>`resource` | query | no | anyOf=#/components/schemas/ApplicationResource \| type="null" |
| <a id="s-92d75808e9"></a>`active` | query | no | anyOf=type="boolean" \| type="null" |

### Responses

| Status | Description |
|---|---|
| <a id="s-9a0e80f83f"></a>`200` | Successful Response |
| <a id="s-94ffa37439"></a>`400` | Bad Request |
| <a id="s-070b17e6f3"></a>`401` | Unauthorized |
| <a id="s-dd33089161"></a>`403` | Forbidden |
| <a id="s-fd31092711"></a>`500` | Internal Server Error |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}; reason="bounded-route-progression"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [GET /v1/app-key-access](#s-7fc99fa765) | `logical-result-cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-d4416783ec"></a>[parameter page_size](#s-34884d05d9) | `value · schema-value · contract_max` | shared above |

## Maintained corroboration

### Related interface records

- [Operation parity: list_app_key_access](../operation/operation-parity-list-app-key-access.md)

### Referenced contract dossiers

- [schemas: AppAccessListOut](schemas-appaccesslistout.md)
- [schemas: ApplicationAccessSort](schemas-applicationaccesssort.md)
- [schemas: ApplicationKeyId](schemas-applicationkeyid.md)
- [schemas: ApplicationName](schemas-applicationname.md)
- [schemas: ApplicationPermission](schemas-applicationpermission.md)
- [schemas: ApplicationResource](schemas-applicationresource.md)
- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
- [schemas: BrowseQuery](schemas-browsequery.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: SortOrder](schemas-sortorder.md)

## Governing policies

- <a id="pa-bd7ca06549"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-da9e03f342"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)
- <a id="pa-020c6b1a0f"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1app-key-access/get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 21ce7f8e1386e06cf46b7c65772cbbea50f3fb18722f10cd992c64135c3c8d9f -->

```json
{
  "operationId": "list_app_key_access",
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
      "name": "sort",
      "required": false,
      "schema": {
        "$ref": "#/components/schemas/ApplicationAccessSort",
        "default": "permission"
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
      "name": "app",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "$ref": "#/components/schemas/ApplicationName"
          },
          {
            "type": "null"
          }
        ],
        "title": "App"
      }
    },
    {
      "in": "query",
      "name": "key",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "$ref": "#/components/schemas/ApplicationKeyId"
          },
          {
            "type": "null"
          }
        ],
        "title": "Key"
      }
    },
    {
      "in": "query",
      "name": "permission",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "$ref": "#/components/schemas/ApplicationPermission"
          },
          {
            "type": "null"
          }
        ],
        "title": "Permission"
      }
    },
    {
      "in": "query",
      "name": "resource",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "$ref": "#/components/schemas/ApplicationResource"
          },
          {
            "type": "null"
          }
        ],
        "title": "Resource"
      }
    },
    {
      "in": "query",
      "name": "active",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "type": "boolean"
          },
          {
            "type": "null"
          }
        ],
        "title": "Active"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/AppAccessListOut"
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
  "summary": "List App Key Access",
  "tags": [
    "apps"
  ],
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "keys:manage"
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
