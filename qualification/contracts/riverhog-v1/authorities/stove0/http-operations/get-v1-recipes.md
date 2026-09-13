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
- <a id="s-eff44f9288"></a>`operationId`: list_recipes
- <a id="s-8568b0786d"></a>`summary`: List Recipes

### Responses

| Status | Description |
|---|---|
| <a id="s-a3267f057b"></a>`200` | Successful Response |
| <a id="s-cee290a15f"></a>`400` | Bad Request |
| <a id="s-553f3580e8"></a>`401` | Unauthorized |
| <a id="s-4a2af01f18"></a>`403` | Forbidden |
| <a id="s-f1ddbd3b59"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [stove0-client recipe list](../../stove0-client/cli/stove0-client-recipe-list.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: RecipeCatalogView](../http-schemas/schemas-recipecatalogview.md)

## Governing policies

- <a id="pa-0e5c4ab99c"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Operation qualification evidence

This evidence proves maintained client, CLI, response-authority, and provider qualification without creating a second semantic operation.

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "recipe list"
  ],
  "client": "Stove0ApiClient",
  "method": "GET",
  "operation_id": "list_recipes",
  "path": "/v1/recipes",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1recipes/get`

### Exact owned JSON

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
