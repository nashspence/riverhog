# GET /v1/collections/{collection_id}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:get-v1-collections-collection-id:3c0f6dd4a6 -->

Get Collection

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-90ea1a2611"></a>
- <a id="s-3945dfc1c9"></a>`operationId`: `"get_collection"`
- <a id="s-bdcc37d19c"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-55e86ebe25"></a>`summary`: `"Get Collection"`
- <a id="s-8ffd78ace0"></a>`tags`: `["collections"]`
- <a id="s-2c623f8ea5"></a>`x-riverhog-permission-requirements`: `[{"any_of":["catalog:read"]}]`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-4f313b0704"></a>`collection_id` | path | yes | not declared | type="integer"; minimum=1; title="Collection Id" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-0efbe086d7"></a>`200` | Successful Response | application/json | [CollectionSummaryOut](../http-schemas/schemas-collectionsummaryout.md) | not declared |
| <a id="s-e028a9e6b6"></a>`400` | Bad Request | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `bad_request` |
| <a id="s-7f0ab83c2e"></a>`401` | Unauthorized | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `unauthorized` |
| <a id="s-1ad2ac4b6a"></a>`403` | Forbidden | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `forbidden` |
| <a id="s-94c0ae4fd7"></a>`404` | Not Found | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `not_found` |
| <a id="s-cdeaeae335"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |

## Maintained corroboration

### Related interface records

- [piggity collection describe](../../piggity/cli/piggity-collection-describe.md)
- [piggity collection show](../../piggity/cli/piggity-collection-show.md)
- [piggity collection tag add](../../piggity/cli/piggity-collection-tag-add.md)
- [piggity collection tag contains](../../piggity/cli/piggity-collection-tag-contains.md)
- [piggity collection tag list](../../piggity/cli/piggity-collection-tag-list.md)
- [piggity collection tag remove](../../piggity/cli/piggity-collection-tag-remove.md)
- [piggity local add](../../piggity/cli/piggity-local-add.md)
- [piggity local repair](../../piggity/cli/piggity-local-repair.md)
- [piggity local sync](../../piggity/cli/piggity-local-sync.md)
- [riverhog_client.ApiClient.get_collection](../../riverhog-client/python/riverhog-client-apiclient-get-collection.md)

### Referenced contract elements

- [schemas: CollectionSummaryOut](../http-schemas/schemas-collectionsummaryout.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)

## Governing policies

- <a id="pa-ea6394cecc"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [riverhog/src/riverhog\_api/routers/collections.py::get\_collection](../../../../../../riverhog/src/riverhog_api/routers/collections.py#L622)

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
      "command": "collection show",
      "executable": "piggity",
      "result_identity": "piggity-cli-result/collection/show/v1",
      "source": {
        "line": 2525,
        "module": "piggity.main",
        "path": "reference/riverhog/applications/piggity/src/piggity/main.py",
        "symbol": "show_cmd"
      }
    },
    {
      "command": "collection describe",
      "executable": "piggity",
      "result_identity": "piggity-cli-result/collection/describe/v1",
      "source": {
        "line": 2536,
        "module": "piggity.main",
        "path": "reference/riverhog/applications/piggity/src/piggity/main.py",
        "symbol": "collection_describe_cmd"
      }
    },
    {
      "command": "collection tag list",
      "executable": "piggity",
      "result_identity": "piggity-cli-result/collection/tag/list/v1",
      "source": {
        "line": 998,
        "module": "piggity.main",
        "path": "reference/riverhog/applications/piggity/src/piggity/main.py",
        "symbol": "collection_tag_list_cmd"
      }
    },
    {
      "command": "collection tag contains",
      "executable": "piggity",
      "result_identity": "piggity-cli-result/collection/tag/contains/v1",
      "source": {
        "line": 1030,
        "module": "piggity.main",
        "path": "reference/riverhog/applications/piggity/src/piggity/main.py",
        "symbol": "collection_tag_contains_cmd"
      }
    },
    {
      "command": "collection tag add",
      "executable": "piggity",
      "result_identity": "piggity-cli-result/collection/tag/add/v1",
      "source": {
        "line": 1057,
        "module": "piggity.main",
        "path": "reference/riverhog/applications/piggity/src/piggity/main.py",
        "symbol": "collection_tag_add_cmd"
      }
    },
    {
      "command": "collection tag remove",
      "executable": "piggity",
      "result_identity": "piggity-cli-result/collection/tag/remove/v1",
      "source": {
        "line": 1086,
        "module": "piggity.main",
        "path": "reference/riverhog/applications/piggity/src/piggity/main.py",
        "symbol": "collection_tag_remove_cmd"
      }
    },
    {
      "command": "local add",
      "executable": "piggity",
      "result_identity": "piggity-cli-result/local/add/v1",
      "source": {
        "line": 902,
        "module": "piggity.local",
        "path": "reference/riverhog/applications/piggity/src/piggity/local.py",
        "symbol": "add_collection"
      }
    },
    {
      "command": "local sync",
      "executable": "piggity",
      "result_identity": "piggity-cli-result/local/sync/v1",
      "source": {
        "line": 1101,
        "module": "piggity.local",
        "path": "reference/riverhog/applications/piggity/src/piggity/local.py",
        "symbol": "sync"
      }
    },
    {
      "command": "local repair",
      "executable": "piggity",
      "result_identity": "piggity-cli-result/local/repair/v1",
      "source": {
        "line": 1122,
        "module": "piggity.local",
        "path": "reference/riverhog/applications/piggity/src/piggity/local.py",
        "symbol": "repair"
      }
    }
  ],
  "cli_commands": [
    "collection describe",
    "collection show",
    "collection tag add",
    "collection tag contains",
    "collection tag list",
    "collection tag remove",
    "local add",
    "local repair",
    "local sync"
  ],
  "client": "ApiClient",
  "client_bindings": [
    {
      "public_identity": "riverhog_client.ApiClient.get_collection",
      "source": {
        "line": 1572,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.get_collection"
      }
    }
  ],
  "method": "GET",
  "operation_id": "get_collection",
  "path": "/v1/collections/{collection_id}",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collections~1{collection_id}/get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 286371f0b8ead838a340a1bb7342cdef26300a38b3fa55ec984a967061c60968 -->

```json
{
  "operationId": "get_collection",
  "parameters": [
    {
      "in": "path",
      "name": "collection_id",
      "required": true,
      "schema": {
        "minimum": 1,
        "title": "Collection Id",
        "type": "integer"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/CollectionSummaryOut"
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
    "404": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Not Found",
      "x-riverhog-error-codes": [
        "not_found"
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
  "summary": "Get Collection",
  "tags": [
    "collections"
  ],
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
