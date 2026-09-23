# GET /v1/recipes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:stove0:get-v1-recipes:3db0b2d1d9 -->

List Recipes

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-e085eb717d"></a>
- <a id="s-eff44f9288"></a>`operationId`: `"list_recipes"`
- <a id="s-8568b0786d"></a>`summary`: `"List Recipes"`
- <a id="s-f2d51c16d3"></a>`tags`: `["recipes"]`

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-a3267f057b"></a>`200` | Successful Response | application/json | [RecipeCatalogView](../http-schemas/schemas-recipecatalogview.md) | not declared |
| <a id="s-cee290a15f"></a>`400` | Bad Request | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `bad_request` |
| <a id="s-553f3580e8"></a>`401` | Unauthorized | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `unauthorized` |
| <a id="s-4a2af01f18"></a>`403` | Forbidden | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `forbidden` |
| <a id="s-f1ddbd3b59"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |

## Maintained corroboration

### Related interface records

- [stove0 recipe list](../../a-stove0-cli/cli/stove0-recipe-list.md)
- [stove0_api_client.Stove0ApiClient.list_recipes](../../stove0-api-client/python/stove0-api-client-stove0apiclient-list-recipes.md)

### Referenced contract elements

- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: RecipeCatalogView](../http-schemas/schemas-recipecatalogview.md)

## Governing policies

- <a id="pa-0e5c4ab99c"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [some-implementations/stove0/application/server/src/stove0\_api/app.py::create\_app.&lt;locals&gt;.list\_recipes](../../../../../../some-implementations/stove0/application/server/src/stove0_api/app.py#L547)

### Structural operation bindings

This generated record links maintained client, CLI, response-authority, and provider routes. It checks interface structure, not executed qualification, successful CLI execution, or human/JSON equivalence. Test bindings and qualification commands are audit leads, not run results.

<details>
<summary>Exact structural binding record</summary>

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_bindings": [
    {
      "command": "recipe list",
      "executable": "stove0",
      "result_identity": "stove0-cli-result/recipe/list/v1",
      "source": {
        "line": 169,
        "module": "a_stove0_cli.main",
        "path": "some-implementations/stove0/application/client/src/a_stove0_cli/main.py",
        "symbol": "list_recipes"
      }
    }
  ],
  "cli_commands": [
    "recipe list"
  ],
  "client": "Stove0ApiClient",
  "client_bindings": [
    {
      "public_identity": "stove0_api_client.Stove0ApiClient.list_recipes",
      "source": {
        "line": 146,
        "module": "stove0_api_client.client",
        "path": "some-implementations/stove0/packages/api-client/src/stove0_api_client/client.py",
        "symbol": "Stove0ApiClient.list_recipes"
      }
    }
  ],
  "method": "GET",
  "operation_id": "list_recipes",
  "path": "/v1/recipes",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1recipes/get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 342876edb19e22f4b606902e0a552b4710f29e12d688642f368a25b291a0216d -->

```json
{
  "operationId": "list_recipes",
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/RecipeCatalogView"
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
  "summary": "List Recipes",
  "tags": [
    "recipes"
  ]
}
```

</details>
