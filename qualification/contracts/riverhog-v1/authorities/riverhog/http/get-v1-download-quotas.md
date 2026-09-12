# GET /v1/download-quotas

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:get-v1-download-quotas:5d9557f411 -->

List Download Quotas

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [download-quotas](families/download-quotas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-64ebb1eab6"></a>
- <a id="s-db2b3b65b9"></a>`operationId`: list_download_quotas
- <a id="s-887bd4832d"></a>`summary`: List Download Quotas
- <a id="s-b2ea6e5552"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-bf5a76fe35"></a>`page_size` | query | no | type="integer"; minimum=1; maximum=100 |
| <a id="s-5b0ea802cc"></a>`page_token` | query | no | anyOf=#/components/schemas/BrowsePageToken \| type="null" |
| <a id="s-1e0b21dfc5"></a>`sort` | query | no | $ref="#/components/schemas/DownloadQuotaSort" |
| <a id="s-b28e08ffe9"></a>`order` | query | no | $ref="#/components/schemas/SortOrder" |
| <a id="s-b29b8e0ba2"></a>`q` | query | no | anyOf=#/components/schemas/BrowseQuery \| type="null" |
| <a id="s-06c5d460e7"></a>`app` | query | no | anyOf=#/components/schemas/ApplicationName \| type="null" |
| <a id="s-7a2a85af8b"></a>`active` | query | no | anyOf=type="boolean" \| type="null" |

### Responses

| Status | Description |
|---|---|
| <a id="s-514d9c191d"></a>`200` | Successful Response |
| <a id="s-33a1603a36"></a>`400` | Bad Request |
| <a id="s-6ca38d9b26"></a>`401` | Unauthorized |
| <a id="s-8e2a78f382"></a>`403` | Forbidden |
| <a id="s-aa1663d8f4"></a>`500` | Internal Server Error |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}; reason="bounded-route-progression"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [GET /v1/download-quotas](#s-64ebb1eab6) | `logical-result-cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-a39854d6cd"></a>[parameter page_size](#s-bf5a76fe35) | `value · schema-value · contract_max` | shared above |

## Maintained corroboration

### Related interface records

- [Operation parity: list_download_quotas](../operation/operation-parity-list-download-quotas.md)

### Referenced contract dossiers

- [schemas: ApplicationName](schemas-applicationname.md)
- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
- [schemas: BrowseQuery](schemas-browsequery.md)
- [schemas: DownloadQuotaSort](schemas-downloadquotasort.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: KeyDownloadQuotaListOut](schemas-keydownloadquotalistout.md)
- [schemas: SortOrder](schemas-sortorder.md)

## Governing policies

- <a id="pa-1ed9671bc6"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-d127923997"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)
- <a id="pa-e94f39e191"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1download-quotas/get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 05cd0b21ee82f26d0514d8d2979d245e7b5f8c15aed48a2deb363dfee741ac37 -->

```json
{
  "operationId": "list_download_quotas",
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
        "$ref": "#/components/schemas/DownloadQuotaSort",
        "default": "app"
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
            "$ref": "#/components/schemas/KeyDownloadQuotaListOut"
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
  "summary": "List Download Quotas",
  "tags": [
    "download quotas"
  ],
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "quotas:manage"
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
