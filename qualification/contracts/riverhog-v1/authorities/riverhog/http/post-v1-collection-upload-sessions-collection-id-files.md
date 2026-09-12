# POST /v1/collection-upload-sessions/{collection_id}/files

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:post-v1-collection-upload-sessions-collec-4a0f562819:c85fc58c50 -->

Register Collection Upload Session Files

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [collection-upload-sessions](families/collection-upload-sessions/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-3ea1e0b8ceab"></a>
- <a id="s-162a814f9dcb"></a>`operationId`: register_collection_upload_session_files
- <a id="s-d9ced11da21f"></a>`summary`: Register Collection Upload Session Files
- <a id="s-da74f9f64ffe"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-5e613c2c9312"></a>`collection_id` | path | yes | type="integer"; minimum=1 |

### <a id="s-316062d2d97f"></a>Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/RegisterCollectionUploadSessionFilesRequest"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| <a id="s-5c415e1b1050"></a>`200` | Successful Response |
| <a id="s-399e5eb75e9a"></a>`400` | Bad Request |
| <a id="s-4cf05f30f044"></a>`401` | Unauthorized |
| <a id="s-0cba83cb9ff6"></a>`403` | Forbidden |
| <a id="s-bd106d20ad58"></a>`404` | Not Found |
| <a id="s-1abde6fa55e6"></a>`409` | Conflict |
| <a id="s-118300d2077d"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: register_collection_upload_session_files](../operation/operation-parity-register-collection-upload-session-files.md)

### Referenced contract dossiers

- [schemas: CollectionUploadSessionFilesRegistrationOut](schemas-collectionuploadsessionfilesregistrationout.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: RegisterCollectionUploadSessionFilesRequest](schemas-registercollectionuploadsessionfilesrequest.md)

## Governing policies

- <a id="pa-8a7dad4f35c1"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collection-upload-sessions~1{collection_id}~1files/post`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ba3c99757013063ebcf2a7a7c24975e37f12ca3545fcbbd3874f843bf23a9f81 -->

```json
{
  "operationId": "register_collection_upload_session_files",
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
          "$ref": "#/components/schemas/RegisterCollectionUploadSessionFilesRequest"
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
            "$ref": "#/components/schemas/CollectionUploadSessionFilesRegistrationOut"
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
    "409": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Conflict",
      "x-riverhog-error-codes": [
        "conflict"
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
  "summary": "Register Collection Upload Session Files",
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
