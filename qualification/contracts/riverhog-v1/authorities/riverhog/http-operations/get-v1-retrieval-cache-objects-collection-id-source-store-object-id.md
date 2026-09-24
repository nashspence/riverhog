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
| <a id="s-3f640503ff"></a>`collection_id` | path | yes | not declared | allOf=[(type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])"); (not=(const="0"))]; title="Collection Id" |
| <a id="s-b5fc534559"></a>`source_store` | path | yes | not declared | type="string"; pattern="^[a-z0-9]+(?:-[a-z0-9]+)*$"; title="Source Store" |
| <a id="s-847eae74a7"></a>`object_id` | path | yes | not declared | type="string"; title="Object Id" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-ffc0a1dc71"></a>`200` | Successful Response | application/json | [RetrievalCacheObjectOut](../http-schemas/schemas-retrievalcacheobjectout.md) | not declared |
| <a id="s-dff249924e"></a>`400` | Bad Request | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `bad_request` |
| <a id="s-8a8490528c"></a>`401` | Unauthorized | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `unauthorized` |
| <a id="s-c757eebef1"></a>`403` | Forbidden | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `forbidden` |
| <a id="s-610fc23789"></a>`404` | Not Found | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `not_found` |
| <a id="s-948f582d70"></a>`500` | Internal Server Error | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `internal_error` |

## Maintained corroboration

### Related interface records

- [a-riverhog-cli retrieval cache show](../../a-riverhog-cli/cli/a-riverhog-cli-retrieval-cache-show.md)
- [riverhog_client.ApiClient.get_retrieval_cache_object](../../riverhog-client/python/riverhog-client-apiclient-get-retrieval-cache-object.md)

### Referenced contract elements

- [schemas: ErrorOut](../http-schemas/schemas-errorout.md)
- [schemas: RetrievalCacheObjectOut](../http-schemas/schemas-retrievalcacheobjectout.md)

## Governing policies

- <a id="pa-e1a14187c5"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [riverhog/src/riverhog\_api/routers/retrieval.py::get\_retrieval\_cache\_object](../../../../../../riverhog/src/riverhog_api/routers/retrieval.py#L141)

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
      "executable": "a-riverhog-cli",
      "result_identity": "a-riverhog-cli-result/retrieval/cache/show/v1",
      "source": {
        "line": 2865,
        "module": "a_riverhog_cli.main",
        "path": "some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py",
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
        "line": 965,
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

<!-- exact-contract-value: 19a9f6bbe3c7c7b6fcde55a2b8fcdc951ca9366cef1f9d28a25fdbd3f3bc1089 -->

```json
{
  "operationId": "get_retrieval_cache_object",
  "parameters": [
    {
      "in": "path",
      "name": "collection_id",
      "required": true,
      "schema": {
        "allOf": [
          {
            "pattern": "^(?:0|[1-9][0-9]{0,17}|[1-8][0-9]{18}|9[0-1][0-9]{17}|92[0-1][0-9]{16}|922[0-2][0-9]{15}|9223[0-2][0-9]{14}|92233[0-6][0-9]{13}|922337[0-1][0-9]{12}|92233720[0-2][0-9]{10}|922337203[0-5][0-9]{9}|9223372036[0-7][0-9]{8}|92233720368[0-4][0-9]{7}|922337203685[0-3][0-9]{6}|9223372036854[0-6][0-9]{5}|92233720368547[0-6][0-9]{4}|922337203685477[0-4][0-9]{3}|9223372036854775[0-7][0-9]{2}|922337203685477580[0-6][0-9]{0}|9223372036854775807)(?![\\s\\S])",
            "type": "string"
          },
          {
            "not": {
              "const": "0"
            }
          }
        ],
        "title": "Collection Id"
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
    "404": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorOut"
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
