# GET /v1/catalog-sync/collections

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:get-v1-catalog-sync-collections:1a92a5f165 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `catalog-sync` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1catalog-sync~1collections/get`

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

- [Operation parity: list_catalog_sync_collections](../operation/operation-parity-list-catalog-sync-collections.md)

## Referenced contract dossiers

- [schemas: CatalogSyncCollectionPage](schemas-catalogsynccollectionpage.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| logical-result-cardinality | items | `segmented_no_total_max` | reason=bounded-route-progression |
| length | characters | `contract_max` | maximum=4096, minimum=1, reason=schema-maximum |
| value | schema-value | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |

## Contract summary

- `operationId`: list_catalog_sync_collections
- `summary`: List Catalog Sync Collections
- `security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| `cursor` | query | yes | string |
| `limit` | query | no | integer |

### Responses

| Status | Description |
|---|---|
| `200` | Successful Response |
| `400` | Bad Request |
| `401` | Unauthorized |
| `403` | Forbidden |
| `409` | Conflict |
| `410` | Gone |
| `500` | Internal Server Error |

## Complete owned contract

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
