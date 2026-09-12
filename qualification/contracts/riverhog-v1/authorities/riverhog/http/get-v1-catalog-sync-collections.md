# GET /v1/catalog-sync/collections

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:get-v1-catalog-sync-collections:1a92a5f165 -->

List Catalog Sync Collections

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [catalog-sync](families/catalog-sync/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

<a id="s-50e16389fb"></a>
- <a id="s-6c00e5f279"></a>`operationId`: list_catalog_sync_collections
- <a id="s-d40fdd6169"></a>`summary`: List Catalog Sync Collections
- <a id="s-1190c6ed4c"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-fc78ce4e09"></a>`cursor` | query | yes | type="string"; minLength=1; maxLength=4096 |
| <a id="s-bb10c7dc50"></a>`limit` | query | no | type="integer"; minimum=1; maximum=100 |

### Responses

| Status | Description |
|---|---|
| <a id="s-a4ea653adc"></a>`200` | Successful Response |
| <a id="s-856ce31e75"></a>`400` | Bad Request |
| <a id="s-19e5d2f0ce"></a>`401` | Unauthorized |
| <a id="s-fe8f7a9a39"></a>`403` | Forbidden |
| <a id="s-2a64b5f010"></a>`409` | Conflict |
| <a id="s-cd19c82c0c"></a>`410` | Gone |
| <a id="s-e8f4d640c1"></a>`500` | Internal Server Error |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: progression={"authority":"catalog-sync-bootstrap","cursor_parameter":"cursor","kind":"exact-authority-page","limit_parameter":"limit"}; reason="bounded-route-progression"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [GET /v1/catalog-sync/collections](#s-50e16389fb) | `logical-result-cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-9308276e6e"></a>[parameter cursor](#s-fc78ce4e09) | `length · characters · contract_max` | maximum=4096 |
| <a id="s-49bc1f8b2f"></a>[parameter limit](#s-bb10c7dc50) | `value · schema-value · contract_max` | maximum=100 |

## Maintained corroboration

### Related interface records

- [Operation parity: list_catalog_sync_collections](../operation/operation-parity-list-catalog-sync-collections.md)

### Referenced contract dossiers

- [schemas: CatalogSyncCollectionPage](schemas-catalogsynccollectionpage.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)

## Governing policies

- <a id="pa-db5d86ac67"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-96b305cfbb"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)
- <a id="pa-c8c1210749"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1catalog-sync~1collections/get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7ba4ba5dbdaa71d29c9b6b43c41b7d1193f6c218010108ff2e636ec1f2bd8a84 -->

```json
{
  "operationId": "list_catalog_sync_collections",
  "parameters": [
    {
      "in": "query",
      "name": "cursor",
      "required": true,
      "schema": {
        "maxLength": 4096,
        "minLength": 1,
        "title": "Cursor",
        "type": "string"
      }
    },
    {
      "in": "query",
      "name": "limit",
      "required": false,
      "schema": {
        "default": 100,
        "maximum": 100,
        "minimum": 1,
        "title": "Limit",
        "type": "integer"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/CatalogSyncCollectionPage"
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
        "catalog_sync_source_changed",
        "catalog_sync_view_changed"
      ]
    },
    "410": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Gone",
      "x-riverhog-error-codes": [
        "catalog_sync_cursor_expired",
        "catalog_sync_history_expired"
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
  "summary": "List Catalog Sync Collections",
  "tags": [
    "catalog synchronization"
  ],
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "catalog:read"
      ]
    }
  ],
  "x-riverhog-read-collection": {
    "authority": "catalog-sync-bootstrap",
    "cursor_parameter": "cursor",
    "kind": "exact-authority-page",
    "limit_parameter": "limit"
  }
}
```
