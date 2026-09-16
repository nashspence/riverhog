# GET /v1/retrieval-cache/objects/{collection_id}/{source_store}/{object_id}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:get-v1-retrieval-cache-objects-collection-bf09699679:18e27dd629 -->

Get Retrieval Cache Object

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-9a8ec3392d"></a>
- <a id="s-ef02a9d664"></a>`operationId`: `"get_retrieval_cache_object"`
- <a id="s-1a2535641b"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-3da411fdfb"></a>`summary`: `"Get Retrieval Cache Object"`
- <a id="s-3b97f3ce95"></a>`tags`: `["retrieval"]`
- <a id="s-1cfb9e29b0"></a>`x-riverhog-permission-requirements`: `[{"any_of":["catalog:read"]}]`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-3f640503ff"></a>`collection_id` | path | yes | not declared | type="integer"; minimum=1; title="Collection Id" |
| <a id="s-b5fc534559"></a>`source_store` | path | yes | not declared | type="string"; pattern="^[a-z0-9]+(?:-[a-z0-9]+)*$"; title="Source Store" |
| <a id="s-847eae74a7"></a>`object_id` | path | yes | not declared | type="string"; title="Object Id" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-ffc0a1dc71"></a>`200` | Successful Response | application/json | [RetrievalCacheObjectOut](../http-schemas/schemas-retrievalcacheobjectout.md) | not declared |
| <a id="s-dff249924e"></a>`400` | Bad Request | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `bad_request` |
| <a id="s-8a8490528c"></a>`401` | Unauthorized | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `unauthorized` |
| <a id="s-c757eebef1"></a>`403` | Forbidden | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `forbidden` |
| <a id="s-610fc23789"></a>`404` | Not Found | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `not_found` |
| <a id="s-948f582d70"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |

## Maintained corroboration

### Related interface records

- [piggity retrieval cache show](../../piggity/cli/piggity-retrieval-cache-show.md)
- [riverhog_client.ApiClient.get_retrieval_cache_object](../../riverhog-client/python/riverhog-client-apiclient-get-retrieval-cache-object.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: RetrievalCacheObjectOut](../http-schemas/schemas-retrievalcacheobjectout.md)

## Governing policies

- <a id="pa-e1a14187c5"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **OpenAPI authority:** [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9)
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`
- **Handler:** [riverhog/src/riverhog_api/routers/retrieval.py::get_retrieval_cache_object](../../../../../../riverhog/src/riverhog_api/routers/retrieval.py#L130)

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
      "command": "retrieval cache show",
      "source": {
        "line": 2850,
        "module": "piggity.main",
        "path": "reference/riverhog/applications/piggity/src/piggity/main.py",
        "symbol": "retrieval_cache_show_cmd"
      }
    }
  ],
  "cli_commands": [
    "retrieval cache show"
  ],
  "client": "ApiClient",
  "client_bindings": [
    {
      "public_identity": "riverhog_client.ApiClient.get_retrieval_cache_object",
      "source": {
        "line": 956,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.get_retrieval_cache_object"
      }
    }
  ],
  "method": "GET",
  "operation_id": "get_retrieval_cache_object",
  "path": "/v1/retrieval-cache/objects/{collection_id}/{source_store}/{object_id}",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1retrieval-cache~1objects~1{collection_id}~1{source_store}~1{object_id}/get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4c5ed7750767a908eece2b94a833375a8a98444cb5e3b8c3e5dd14d1f9425a9b -->

```json
{
  "operationId": "get_retrieval_cache_object",
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
      "name": "source_store",
      "required": true,
      "schema": {
        "pattern": "^[a-z0-9]+(?:-[a-z0-9]+)*$",
        "title": "Source Store",
        "type": "string"
      }
    },
    {
      "in": "path",
      "name": "object_id",
      "required": true,
      "schema": {
        "title": "Object Id",
        "type": "string"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/RetrievalCacheObjectOut"
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
  "summary": "Get Retrieval Cache Object",
  "tags": [
    "retrieval"
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
