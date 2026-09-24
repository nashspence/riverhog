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
- <a id="s-acd042aeac"></a>`operationId`: `"create_catalog_sync_checkpoint"`
- <a id="s-2f98f2f949"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-78727b864e"></a>`summary`: `"Create Catalog Sync Checkpoint"`
- <a id="s-bc06248f6c"></a>`tags`: `["catalog synchronization"]`
- <a id="s-ba9f1acc63"></a>`x-riverhog-interface`: `"client-only-primitive"`
- <a id="s-c792899163"></a>`x-riverhog-permission-requirements`: `[{"any_of":["catalog:read"]}]`

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-d00c372b8d"></a>`200` | Successful Response | application/json | [CatalogSyncCheckpoint](../http-schemas/schemas-catalogsynccheckpoint.md) | not declared |
| <a id="s-ddbf280258"></a>`400` | Bad Request | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `bad_request` |
| <a id="s-333d129fc5"></a>`401` | Unauthorized | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `unauthorized` |
| <a id="s-df041cd847"></a>`403` | Forbidden | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `forbidden` |
| <a id="s-40b0712f12"></a>`500` | Internal Server Error | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `internal_error` |

## Maintained corroboration

### Related interface records

- [a-riverhog-cli catalog-sync checkpoint](../../a-riverhog-cli/cli/a-riverhog-cli-catalog-sync-checkpoint.md)
- [riverhog_client.ApiClient.create_catalog_sync_checkpoint](../../riverhog-client/python/riverhog-client-apiclient-create-catalog-sync-checkpoint.md)

### Referenced contract elements

- [schemas: CatalogSyncCheckpoint](../http-schemas/schemas-catalogsynccheckpoint.md)
- [schemas: ErrorOut](../http-schemas/schemas-errorout.md)

## Governing policies

- <a id="pa-4560f0635d"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [riverhog/src/riverhog\_api/routers/catalog\_sync.py::create\_catalog\_sync\_checkpoint](../../../../../../riverhog/src/riverhog_api/routers/catalog_sync.py#L31)

### Structural operation bindings

This generated record links maintained client, CLI, response-authority, and provider routes. It checks interface structure, not executed qualification, successful CLI execution, or human/JSON equivalence. Test bindings and qualification commands are audit leads, not run results.

<details>
<summary>Exact structural binding record</summary>

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_bindings": [
    {
      "command": "catalog-sync checkpoint",
      "executable": "a-riverhog-cli",
      "result_identity": "a-riverhog-cli-result/catalog-sync/checkpoint/v1",
      "source": {
        "line": 772,
        "module": "a_riverhog_cli.main",
        "path": "some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py",
        "symbol": "catalog_sync_checkpoint_cmd"
      }
    }
  ],
  "cli_commands": [
    "catalog-sync checkpoint"
  ],
  "client": "ApiClient",
  "client_bindings": [
    {
      "public_identity": "riverhog_client.ApiClient.create_catalog_sync_checkpoint",
      "source": {
        "line": 739,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.create_catalog_sync_checkpoint"
      }
    }
  ],
  "method": "GET",
  "operation_id": "create_catalog_sync_checkpoint",
  "path": "/v1/catalog-sync/checkpoint",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1catalog-sync~1checkpoint/get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: eb5c6fa7c96e62b14f75526161a78b47a9194cbe3e6536764f00c144e936243a -->

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
            "$ref": "#/components/schemas/ErrorOut"
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
            "$ref": "#/components/schemas/ErrorOut"
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
            "$ref": "#/components/schemas/ErrorOut"
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
            "$ref": "#/components/schemas/ErrorOut"
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

</details>
