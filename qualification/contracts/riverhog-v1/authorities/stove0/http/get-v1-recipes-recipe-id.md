# GET /v1/recipes/{recipe_id}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:get-v1-recipes-recipe-id:d3bdcc8c7b -->

Get Recipe

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [recipes](families/recipes/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-2ed760441c"></a>
- <a id="s-721e8eec1f"></a>`operationId`: get_recipe
- <a id="s-0aba5a30b1"></a>`summary`: Get Recipe

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-6eecb65b87"></a>`recipe_id` | path | yes | type="string" |
| <a id="s-220fc4fec4"></a>`revision` | query | no | anyOf=type="integer" \| type="null" |

### Responses

| Status | Description |
|---|---|
| <a id="s-ffae93a9f9"></a>`200` | Successful Response |
| <a id="s-65ca6825ca"></a>`400` | Bad Request |
| <a id="s-c9702de05e"></a>`401` | Unauthorized |
| <a id="s-67220f4666"></a>`403` | Forbidden |
| <a id="s-b3e1dbfa69"></a>`404` | Not Found |
| <a id="s-82071bf9f7"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: get_recipe](../operation/operation-parity-get-recipe.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: RecipeView](schemas-recipeview.md)

## Governing policies

- <a id="pa-7ee2e12239"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1recipes~1{recipe_id}/get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9262bc9cfee6f570eaee884563e726501ec0dcecf1a62dca30416ef13a97ebea -->

```json
{
  "operationId": "get_recipe",
  "parameters": [
    {
      "in": "path",
      "name": "recipe_id",
      "required": true,
      "schema": {
        "title": "Recipe Id",
        "type": "string"
      }
    },
    {
      "in": "query",
      "name": "revision",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "type": "integer"
          },
          {
            "type": "null"
          }
        ],
        "title": "Revision"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/RecipeView"
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
  "summary": "Get Recipe",
  "tags": [
    "recipes"
  ]
}
```
