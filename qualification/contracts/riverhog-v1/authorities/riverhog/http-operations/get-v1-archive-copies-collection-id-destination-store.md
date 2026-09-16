# GET /v1/archive/copies/{collection_id}/{destination_store}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:get-v1-archive-copies-collection-id-desti-954373ca14:6ccd95d8c9 -->

Get Archive Copy Job

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-de5ffe3a1e"></a>
- <a id="s-7bbf4e456f"></a>`operationId`: `"get_archive_copy_job"`
- <a id="s-d37af7c443"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-346551dd72"></a>`summary`: `"Get Archive Copy Job"`
- <a id="s-c508fbb064"></a>`tags`: `["archive"]`
- <a id="s-6637dc7f3e"></a>`x-riverhog-permission-requirements`: `[{"any_of":["archives:manage"]}]`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-05756f1e25"></a>`collection_id` | path | yes | not declared | type="integer"; minimum=1; title="Collection Id" |
| <a id="s-ee083b4c14"></a>`destination_store` | path | yes | not declared | type="string"; pattern="^[a-z0-9]+(?:-[a-z0-9]+)*$"; title="Destination Store" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-7ffcd84564"></a>`200` | Successful Response | application/json | [ArchiveCopyJobOut](../http-schemas/schemas-archivecopyjobout.md) | not declared |
| <a id="s-3ee6bf46d7"></a>`400` | Bad Request | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `bad_request` |
| <a id="s-6c584e0b68"></a>`401` | Unauthorized | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `unauthorized` |
| <a id="s-a51e7214ef"></a>`403` | Forbidden | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `forbidden` |
| <a id="s-9fd25d8d43"></a>`404` | Not Found | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `not_found` |
| <a id="s-035dda0d46"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |

## Maintained corroboration

### Related interface records

- [piggity archive copy show](../../piggity/cli/piggity-archive-copy-show.md)
- [piggity archive copy watch](../../piggity/cli/piggity-archive-copy-watch.md)
- [riverhog_client.ApiClient.get_archive_copy_job](../../riverhog-client/python/riverhog-client-apiclient-get-archive-copy-job.md)

### Referenced contract dossiers

- [schemas: ArchiveCopyJobOut](../http-schemas/schemas-archivecopyjobout.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)

## Governing policies

- <a id="pa-309a4a6057"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **OpenAPI authority:** [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9)
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`
- **Handler:** [riverhog/src/riverhog_api/routers/archive.py::get_archive_copy_job](../../../../../../riverhog/src/riverhog_api/routers/archive.py#L121)

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
      "command": "archive copy show",
      "source": {
        "line": 3044,
        "module": "piggity.main",
        "path": "reference/riverhog/applications/piggity/src/piggity/main.py",
        "symbol": "archive_copy_show_cmd"
      }
    },
    {
      "command": "archive copy watch",
      "source": {
        "line": 3080,
        "module": "piggity.main",
        "path": "reference/riverhog/applications/piggity/src/piggity/main.py",
        "symbol": "archive_copy_watch_cmd"
      }
    }
  ],
  "cli_commands": [
    "archive copy show",
    "archive copy watch"
  ],
  "client": "ApiClient",
  "client_bindings": [
    {
      "public_identity": "riverhog_client.ApiClient.get_archive_copy_job",
      "source": {
        "line": 2461,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.get_archive_copy_job"
      }
    }
  ],
  "method": "GET",
  "operation_id": "get_archive_copy_job",
  "path": "/v1/archive/copies/{collection_id}/{destination_store}",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1archive~1copies~1{collection_id}~1{destination_store}/get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6dc15f1231d287346e2d6b30ff0c6d90fe98110a8b74f472f84be130d32293ec -->

```json
{
  "operationId": "get_archive_copy_job",
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
    },
    {
      "in": "path",
      "name": "destination_store",
      "required": true,
      "schema": {
        "pattern": "^[a-z0-9]+(?:-[a-z0-9]+)*$",
        "title": "Destination Store",
        "type": "string"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ArchiveCopyJobOut"
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
  "summary": "Get Archive Copy Job",
  "tags": [
    "archive"
  ],
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "archives:manage"
      ]
    }
  ]
}
```

</details>
