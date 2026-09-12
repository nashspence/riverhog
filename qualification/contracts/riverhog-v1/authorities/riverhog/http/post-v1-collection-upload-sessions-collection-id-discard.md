# POST /v1/collection-upload-sessions/{collection_id}/discard

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:post-v1-collection-upload-sessions-collec-d4e0df816c:aa9e184204 -->

Discard Collection Upload

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [collection-upload-sessions](families/collection-upload-sessions/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-84b0d95f8b22"></a>
- <a id="s-28284d09380c"></a>`operationId`: discard_collection_upload
- <a id="s-4f64c70b0238"></a>`summary`: Discard Collection Upload
- <a id="s-ec1bc239b995"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-704921697751"></a>`collection_id` | path | yes | type="integer"; minimum=1 |

### <a id="s-59bf7d4e085f"></a>Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/DiscardCollectionUploadRequest"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| <a id="s-e5b9c9f0dc73"></a>`200` | Successful Response |
| <a id="s-8ad61111b3f8"></a>`400` | Bad Request |
| <a id="s-b7bf99369c67"></a>`401` | Unauthorized |
| <a id="s-b3591cec3ff3"></a>`403` | Forbidden |
| <a id="s-21abc3350536"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: discard_collection_upload](../operation/operation-parity-discard-collection-upload.md)

### Referenced contract dossiers

- [schemas: CollectionUploadDiscardResultOut](schemas-collectionuploaddiscardresultout.md)
- [schemas: DiscardCollectionUploadRequest](schemas-discardcollectionuploadrequest.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)

## Governing policies

- <a id="pa-8407b23df089"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collection-upload-sessions~1{collection_id}~1discard/post`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9a47bf519998931bd24983aca8d8a4289917a983a754dfe345bece5f85e39c0f -->

```json
{
  "operationId": "discard_collection_upload",
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
  "requestBody": {
    "content": {
      "application/json": {
        "schema": {
          "$ref": "#/components/schemas/DiscardCollectionUploadRequest"
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
            "$ref": "#/components/schemas/CollectionUploadDiscardResultOut"
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
  "summary": "Discard Collection Upload",
  "tags": [
    "collections"
  ],
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "collections:delete"
      ]
    }
  ]
}
```
