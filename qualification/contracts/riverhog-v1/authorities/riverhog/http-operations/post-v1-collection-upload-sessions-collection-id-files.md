# POST /v1/collection-upload-sessions/{collection_id}/files

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:post-v1-collection-upload-sessions-collec-4a0f562819:91e49c31c6 -->

Register Collection Upload Session Files

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-3ea1e0b8ce"></a>
- <a id="s-162a814f9d"></a>`operationId`: register_collection_upload_session_files
- <a id="s-d9ced11da2"></a>`summary`: Register Collection Upload Session Files
- <a id="s-da74f9f64f"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-5e613c2c93"></a>`collection_id` | path | yes | type="integer"; minimum=1 |

### <a id="s-316062d2d9"></a>Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/RegisterCollectionUploadSessionFilesRequest"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| <a id="s-5c415e1b10"></a>`200` | Successful Response |
| <a id="s-399e5eb75e"></a>`400` | Bad Request |
| <a id="s-4cf05f30f0"></a>`401` | Unauthorized |
| <a id="s-0cba83cb9f"></a>`403` | Forbidden |
| <a id="s-bd106d20ad"></a>`404` | Not Found |
| <a id="s-1abde6fa55"></a>`409` | Conflict |
| <a id="s-118300d207"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [piggity collection upload start](../../piggity/cli/piggity-collection-upload-start.md)

### Referenced contract dossiers

- [schemas: CollectionUploadSessionFilesRegistrationOut](../http-schemas/schemas-collectionuploadsessionfilesregistrationout.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: RegisterCollectionUploadSessionFilesRequest](../http-schemas/schemas-registercollectionuploadsessionfilesrequest.md)

## Governing policies

- <a id="pa-eb9039d8a8"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

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
  "operation_id": "register_collection_upload_session_files",
  "path": "/v1/collection-upload-sessions/{collection_id}/files",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```

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
