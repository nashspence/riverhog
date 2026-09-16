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
- <a id="s-3945dfc1c9"></a>`operationId`: get_collection
- <a id="s-55e86ebe25"></a>`summary`: Get Collection
- <a id="s-bdcc37d19c"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-4f313b0704"></a>`collection_id` | path | yes | type="integer"; minimum=1 |

### Responses

| Status | Description |
|---|---|
| <a id="s-0efbe086d7"></a>`200` | Successful Response |
| <a id="s-e028a9e6b6"></a>`400` | Bad Request |
| <a id="s-7f0ab83c2e"></a>`401` | Unauthorized |
| <a id="s-1ad2ac4b6a"></a>`403` | Forbidden |
| <a id="s-94c0ae4fd7"></a>`404` | Not Found |
| <a id="s-cdeaeae335"></a>`500` | Internal Server Error |

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

### Referenced contract dossiers

- [schemas: CollectionSummaryOut](../http-schemas/schemas-collectionsummaryout.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)

## Governing policies

- <a id="pa-ea6394cecc"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

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
  "classification": "human-cli+json",
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
  "method": "GET",
  "operation_id": "get_collection",
  "path": "/v1/collections/{collection_id}",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collections~1{collection_id}/get`

### Exact owned JSON

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
