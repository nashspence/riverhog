# GET /v1/catalog-sync/changes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:get-v1-catalog-sync-changes:3d19f95b19 -->

List Catalog Sync Changes

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-dae178486d"></a>
- <a id="s-e3c4a62d1c"></a>`operationId`: `"list_catalog_sync_changes"`
- <a id="s-039231e67a"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-a3916c60e2"></a>`summary`: `"List Catalog Sync Changes"`
- <a id="s-768a1ec713"></a>`tags`: `["catalog synchronization"]`
- <a id="s-877d786b46"></a>`x-riverhog-permission-requirements`: `[{"any_of":["catalog:read"]}]`
- <a id="s-602b3e231c"></a>`x-riverhog-read-collection`: `{"cursor_parameter":"cursor","kind":"cursor-feed","limit_parameter":"limit"}`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-3cd41b6792"></a>`cursor` | query | yes | not declared | type="string"; maxLength=4096; minLength=1; title="Cursor" |
| <a id="s-4c554ae937"></a>`limit` | query | no | `100` | type="integer"; minimum=1; maximum=100; title="Limit" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-965d91e8aa"></a>`200` | Successful Response | application/json | [CatalogSyncChangePage](../http-schemas/schemas-catalogsyncchangepage.md) | not declared |
| <a id="s-24e908e560"></a>`400` | Bad Request | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `bad_request` |
| <a id="s-5d4cfffeb6"></a>`401` | Unauthorized | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `unauthorized` |
| <a id="s-3ab0b60413"></a>`403` | Forbidden | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `forbidden` |
| <a id="s-6a82860991"></a>`409` | Conflict | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `catalog_sync_source_changed`, `catalog_sync_view_changed` |
| <a id="s-4b26bcf39e"></a>`410` | Gone | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `catalog_sync_cursor_expired`, `catalog_sync_history_expired` |
| <a id="s-385f819943"></a>`500` | Internal Server Error | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `internal_error` |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

Shared facts for every subject below: progression={"cursor_parameter":"cursor","kind":"cursor-feed","limit_parameter":"limit"}; reason="bounded-route-progression"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [GET /v1/catalog-sync/changes](#s-dae178486d) | `logical-result-cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-087f8d03e8"></a>[parameter cursor](#s-3cd41b6792) | `length · characters · contract_max` | maximum=4096 |
| <a id="s-399c39dc78"></a>[parameter limit](#s-4c554ae937) | `value · schema-value · contract_max` | maximum=100 |

### Evidence gaps

The named contract groups have recorded evidence gaps in the following guarantees. Each group's page identifies its exact open guarantees and candidate tests:

- Each step stays within its declared limits.
- Continuing the work makes progress toward its declared completion.
- The operation works across multiple pages or chunks.
- Required data or work is not silently left out.
- Work can resume after a restart as its contract requires.

These guarantees let large tasks proceed in smaller steps: a limit on one page or chunk must not become a hidden limit on the whole task. Returning a first page correctly does not establish that continuation or recovery works. Capacity may explicitly reject, defer, or throttle work; it must not silently omit work.

Existing tests may establish individual cases. The gaps retain their recorded group-wide scope and do not establish a bug in every linked contract. Completion follows each contract's rules; mutable browsing carries no implied snapshot guarantee.

Required by: [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb).

Exact evidence groups for this contract element:

- [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md)

## Maintained corroboration

### Related interface records

- [a-riverhog-cli catalog-sync changes](../../a-riverhog-cli/cli/a-riverhog-cli-catalog-sync-changes.md)
- [riverhog_client.ApiClient.list_catalog_sync_changes](../../riverhog-client/python/riverhog-client-apiclient-list-catalog-sync-changes.md)

### Referenced contract elements

- [schemas: CatalogSyncChangePage](../http-schemas/schemas-catalogsyncchangepage.md)
- [schemas: ErrorOut](../http-schemas/schemas-errorout.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-00c01da577"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-30fa77cb42"></a>[extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)
- <a id="pa-54b5467e47"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [riverhog/src/riverhog\_api/routers/catalog\_sync.py::list\_catalog\_sync\_changes](../../../../../../riverhog/src/riverhog_api/routers/catalog_sync.py#L66)

### Structural operation bindings

This generated record links maintained client, CLI, response-authority, and provider routes. It checks interface structure, not executed qualification, successful CLI execution, or human/JSON equivalence. Test bindings and qualification commands are audit leads, not run results.

<details>
<summary>Exact structural binding record</summary>

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_bindings": [
    {
      "command": "catalog-sync changes",
      "executable": "a-riverhog-cli",
      "result_identity": "a-riverhog-cli-result/catalog-sync/changes/v1",
      "source": {
        "line": 815,
        "module": "a_riverhog_cli.main",
        "path": "some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py",
        "symbol": "catalog_sync_changes_cmd"
      }
    }
  ],
  "cli_commands": [
    "catalog-sync changes"
  ],
  "client": "ApiClient",
  "client_bindings": [
    {
      "public_identity": "riverhog_client.ApiClient.list_catalog_sync_changes",
      "source": {
        "line": 759,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.list_catalog_sync_changes"
      }
    }
  ],
  "method": "GET",
  "operation_id": "list_catalog_sync_changes",
  "path": "/v1/catalog-sync/changes",
  "provider_evidence": null,
  "read_collection": {
    "cursor_parameter": "cursor",
    "kind": "cursor-feed",
    "limit_parameter": "limit"
  },
  "response_authority": "canonical-document"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1catalog-sync~1changes/get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f66ad9c8bbefa8fd73200b5b89462c49ee9fbae385dee1c29b1f28d835f844f0 -->

```json
{
  "operationId": "list_catalog_sync_changes",
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
            "$ref": "#/components/schemas/CatalogSyncChangePage"
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
    "409": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorOut"
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
            "$ref": "#/components/schemas/ErrorOut"
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
  "summary": "List Catalog Sync Changes",
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
    "cursor_parameter": "cursor",
    "kind": "cursor-feed",
    "limit_parameter": "limit"
  }
}
```

</details>
