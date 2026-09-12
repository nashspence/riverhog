# PUT /v1/collections/{collection_id}/description

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:put-v1-collections-collection-id-description:9e5a38d458 -->

Replace Collection Description

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [collections](families/collections/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-af8c734346df"></a>
- <a id="s-a7f741748702"></a>`operationId`: replace_collection_description
- <a id="s-88ac2f282100"></a>`summary`: Replace Collection Description
- <a id="s-4d7049a72b5b"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-f298dfd6d7ca"></a>`collection_id` | path | yes | type="integer"; minimum=1 |
| <a id="s-1894750874e0"></a>`If-Match` | header | yes | type="string"; pattern="^\"[0-9a-f]{64}\"$" |

### <a id="s-805a79ced6f5"></a>Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/ReplaceCollectionDescriptionRequest"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| <a id="s-2c0b35907893"></a>`200` | Successful Response |
| <a id="s-29f5bd9918cd"></a>`400` | Bad Request |
| <a id="s-9c4e75c826f2"></a>`401` | Unauthorized |
| <a id="s-05972b7342c5"></a>`403` | Forbidden |
| <a id="s-7be43d128b84"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: replace_collection_description](../operation/operation-parity-replace-collection-description.md)

### Referenced contract dossiers

- [schemas: CollectionDescriptionOut](schemas-collectiondescriptionout.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: ReplaceCollectionDescriptionRequest](schemas-replacecollectiondescriptionrequest.md)

## Governing policies

- <a id="pa-915443cbaba4"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collections~1{collection_id}~1description/put`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: df86985a99a1a99fcec7f7726fc83f1387336c40182059c1c960c150c3cc0dd8 -->

```json
{
  "operationId": "replace_collection_description",
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
      "in": "header",
      "name": "If-Match",
      "required": true,
      "schema": {
        "pattern": "^\"[0-9a-f]{64}\"$",
        "title": "If-Match",
        "type": "string"
      }
    }
  ],
  "requestBody": {
    "content": {
      "application/json": {
        "schema": {
          "$ref": "#/components/schemas/ReplaceCollectionDescriptionRequest"
        }
      }
    },
    "required": true
  },
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/CollectionDescriptionOut"
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
  "security": [
    {
      "HTTPBearer": []
    }
  ],
  "summary": "Replace Collection Description",
  "tags": [
    "collections"
  ],
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "collection-descriptions:manage"
      ]
    }
  ]
}
```
