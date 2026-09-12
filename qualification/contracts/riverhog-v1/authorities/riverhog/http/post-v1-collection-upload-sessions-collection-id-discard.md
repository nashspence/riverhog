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

<a id="s-84b0d95f8b"></a>
- <a id="s-28284d0938"></a>`operationId`: discard_collection_upload
- <a id="s-4f64c70b02"></a>`summary`: Discard Collection Upload
- <a id="s-ec1bc239b9"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-7049216977"></a>`collection_id` | path | yes | type="integer"; minimum=1 |

### <a id="s-59bf7d4e08"></a>Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/DiscardCollectionUploadRequest"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| <a id="s-e5b9c9f0dc"></a>`200` | Successful Response |
| <a id="s-8ad61111b3"></a>`400` | Bad Request |
| <a id="s-b7bf99369c"></a>`401` | Unauthorized |
| <a id="s-b3591cec3f"></a>`403` | Forbidden |
| <a id="s-21abc33505"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: discard_collection_upload](../operation/operation-parity-discard-collection-upload.md)

### Referenced contract dossiers

- [schemas: CollectionUploadDiscardResultOut](schemas-collectionuploaddiscardresultout.md)
- [schemas: DiscardCollectionUploadRequest](schemas-discardcollectionuploadrequest.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)

## Governing policies

- <a id="pa-8407b23df0"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
