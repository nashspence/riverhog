# POST /v1/collection-upload-sessions/{collection_id}/tags

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:post-v1-collection-upload-sessions-collec-ab6864a026:d696558c79 -->

Add Collection Upload Session Tags

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-72abdd6c29"></a>
- <a id="s-8b54116eea"></a>`operationId`: add_collection_upload_session_tags
- <a id="s-d69ae4f891"></a>`summary`: Add Collection Upload Session Tags
- <a id="s-6d1a39c28c"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-a931da540f"></a>`collection_id` | path | yes | type="integer"; minimum=1 |

### <a id="s-a15e689314"></a>Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/AddCollectionUploadTagsRequest"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| <a id="s-8f2986ef41"></a>`200` | Successful Response |
| <a id="s-e7b1d00231"></a>`400` | Bad Request |
| <a id="s-d64597e3ab"></a>`401` | Unauthorized |
| <a id="s-e9de0a8c68"></a>`403` | Forbidden |
| <a id="s-477bc9677c"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [piggity collection upload start](../../piggity/cli/piggity-collection-upload-start.md)

### Referenced contract dossiers

- [schemas: AddCollectionUploadTagsRequest](../http-schemas/schemas-addcollectionuploadtagsrequest.md)
- [schemas: CollectionUploadTagsOut](../http-schemas/schemas-collectionuploadtagsout.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)

## Governing policies

- <a id="pa-3f1de373c6"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Operation qualification evidence

This evidence proves maintained client, CLI, response-authority, and provider qualification without creating a second semantic operation.

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [
    "collection upload start"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "add_collection_upload_session_tags",
  "path": "/v1/collection-upload-sessions/{collection_id}/tags",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collection-upload-sessions~1{collection_id}~1tags/post`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b980184e3b6b35749412f1fc54fd72e28d8ed8b3ad35b236b61a375bf610eeec -->

```json
{
  "operationId": "add_collection_upload_session_tags",
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
          "$ref": "#/components/schemas/AddCollectionUploadTagsRequest"
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
            "$ref": "#/components/schemas/CollectionUploadTagsOut"
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
  "summary": "Add Collection Upload Session Tags",
  "tags": [
    "collections"
  ],
  "x-riverhog-interface": "client-only-primitive",
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "collections:create"
      ]
    }
  ]
}
```
