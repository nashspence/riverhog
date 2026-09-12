# GET /v1/catalog-sync/checkpoint

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:get-v1-catalog-sync-checkpoint:c2273e44e6 -->

Create Catalog Sync Checkpoint

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [catalog-sync](families/catalog-sync/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-817ac9989fe6"></a>
- <a id="s-acd042aeacad"></a>`operationId`: create_catalog_sync_checkpoint
- <a id="s-78727b864e2d"></a>`summary`: Create Catalog Sync Checkpoint
- <a id="s-2f98f2f94992"></a>`security`: `[{"HTTPBearer": []}]`

### Responses

| Status | Description |
|---|---|
| <a id="s-d00c372b8dbb"></a>`200` | Successful Response |
| <a id="s-ddbf280258f3"></a>`400` | Bad Request |
| <a id="s-333d129fc5ec"></a>`401` | Unauthorized |
| <a id="s-df041cd84773"></a>`403` | Forbidden |
| <a id="s-40b0712f1269"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: create_catalog_sync_checkpoint](../operation/operation-parity-create-catalog-sync-checkpoint.md)

### Referenced contract dossiers

- [schemas: CatalogSyncCheckpoint](schemas-catalogsynccheckpoint.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)

## Governing policies

- <a id="pa-ef095ab23dbe"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1catalog-sync~1checkpoint/get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b8ab82a8feabe3888d8e3443f1d8be7127962a0d9d721fc73496eb49abd55aa9 -->

```json
{
  "operationId": "create_catalog_sync_checkpoint",
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/CatalogSyncCheckpoint"
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
  "summary": "Create Catalog Sync Checkpoint",
  "tags": [
    "catalog synchronization"
  ],
  "x-riverhog-interface": "client-only-primitive",
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "catalog:read"
      ]
    }
  ]
}
```
