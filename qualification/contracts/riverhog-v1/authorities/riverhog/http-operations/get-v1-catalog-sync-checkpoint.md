# GET /v1/catalog-sync/checkpoint

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:get-v1-catalog-sync-checkpoint:977ae4945f -->

Create Catalog Sync Checkpoint

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-817ac9989f"></a>
- <a id="s-acd042aeac"></a>`operationId`: create_catalog_sync_checkpoint
- <a id="s-78727b864e"></a>`summary`: Create Catalog Sync Checkpoint
- <a id="s-2f98f2f949"></a>`security`: `[{"HTTPBearer": []}]`

### Responses

| Status | Description |
|---|---|
| <a id="s-d00c372b8d"></a>`200` | Successful Response |
| <a id="s-ddbf280258"></a>`400` | Bad Request |
| <a id="s-333d129fc5"></a>`401` | Unauthorized |
| <a id="s-df041cd847"></a>`403` | Forbidden |
| <a id="s-40b0712f12"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [piggity catalog-sync checkpoint](../../piggity/cli/piggity-catalog-sync-checkpoint.md)

### Referenced contract dossiers

- [schemas: CatalogSyncCheckpoint](../http-schemas/schemas-catalogsynccheckpoint.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)

## Governing policies

- <a id="pa-4560f0635d"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Structural operation bindings

This generated record links maintained client, CLI, response-authority, and provider routes. It checks interface structure, not executed qualification, successful CLI execution, or human/JSON equivalence. Test bindings and qualification commands are audit leads, not run results.

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [
    "catalog-sync checkpoint"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "create_catalog_sync_checkpoint",
  "path": "/v1/catalog-sync/checkpoint",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```

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
