# GET /v1/catalog-sync/collections

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:get-v1-catalog-sync-collections:0c24d21854 -->

List Catalog Sync Collections

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-50e16389fb"></a>
- <a id="s-6c00e5f279"></a>`operationId`: `"list_catalog_sync_collections"`
- <a id="s-1190c6ed4c"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-d40fdd6169"></a>`summary`: `"List Catalog Sync Collections"`
- <a id="s-fc4c0ddcb7"></a>`tags`: `["catalog synchronization"]`
- <a id="s-6579ebd83b"></a>`x-riverhog-permission-requirements`: `[{"any_of":["catalog:read"]}]`
- <a id="s-479876f4ab"></a>`x-riverhog-read-collection`: `{"authority":"catalog-sync-bootstrap","cursor_parameter":"cursor","kind":"exact-authority-page","limit_parameter":"limit"}`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-fc78ce4e09"></a>`cursor` | query | yes | not declared | type="string"; maxLength=4096; minLength=1; title="Cursor" |
| <a id="s-bb10c7dc50"></a>`limit` | query | no | `100` | type="integer"; minimum=1; maximum=100; title="Limit" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-a4ea653adc"></a>`200` | Successful Response | application/json | [CatalogSyncCollectionPage](../http-schemas/schemas-catalogsynccollectionpage.md) | not declared |
| <a id="s-856ce31e75"></a>`400` | Bad Request | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `bad_request` |
| <a id="s-19e5d2f0ce"></a>`401` | Unauthorized | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `unauthorized` |
| <a id="s-fe8f7a9a39"></a>`403` | Forbidden | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `forbidden` |
| <a id="s-2a64b5f010"></a>`409` | Conflict | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `catalog_sync_source_changed`, `catalog_sync_view_changed` |
| <a id="s-cd19c82c0c"></a>`410` | Gone | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `catalog_sync_cursor_expired`, `catalog_sync_history_expired` |
| <a id="s-e8f4d640c1"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

Shared facts for every subject below: progression={"authority":"catalog-sync-bootstrap","cursor_parameter":"cursor","kind":"exact-authority-page","limit_parameter":"limit"}; reason="bounded-route-progression"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [GET /v1/catalog-sync/collections](#s-50e16389fb) | `logical-result-cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-9308276e6e"></a>[parameter cursor](#s-fc78ce4e09) | `length · characters · contract_max` | maximum=4096 |
| <a id="s-49bc1f8b2f"></a>[parameter limit](#s-bb10c7dc50) | `value · schema-value · contract_max` | maximum=100 |

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

- [piggity catalog-sync collections](../../piggity/cli/piggity-catalog-sync-collections.md)
- [riverhog_client.ApiClient.list_catalog_sync_collections](../../riverhog-client/python/riverhog-client-apiclient-list-catalog-sync-collections.md)

### Referenced contract elements

- [schemas: CatalogSyncCollectionPage](../http-schemas/schemas-catalogsynccollectionpage.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-c29e9c84b1"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-56d8c2a7d2"></a>[extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)
- <a id="pa-ebfb8770e5"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [riverhog/src/riverhog\_api/routers/catalog\_sync.py::list\_catalog\_sync\_collections](../../../../../../riverhog/src/riverhog_api/routers/catalog_sync.py#L44)

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
      "command": "catalog-sync collections",
      "executable": "piggity",
      "result_identity": "piggity-cli-result/catalog-sync/collections/v1",
      "source": {
        "line": 787,
        "module": "piggity.main",
        "path": "reference/riverhog/applications/piggity/src/piggity/main.py",
        "symbol": "catalog_sync_collections_cmd"
      }
    }
  ],
  "cli_commands": [
    "catalog-sync collections"
  ],
  "client": "ApiClient",
  "client_bindings": [
    {
      "public_identity": "riverhog_client.ApiClient.list_catalog_sync_collections",
      "source": {
        "line": 739,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.list_catalog_sync_collections"
      }
    }
  ],
  "method": "GET",
  "operation_id": "list_catalog_sync_collections",
  "path": "/v1/catalog-sync/collections",
  "provider_evidence": null,
  "read_collection": {
    "authority": "catalog-sync-bootstrap",
    "cursor_parameter": "cursor",
    "kind": "exact-authority-page",
    "limit_parameter": "limit"
  },
  "response_authority": "canonical-document"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1catalog-sync~1collections/get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
