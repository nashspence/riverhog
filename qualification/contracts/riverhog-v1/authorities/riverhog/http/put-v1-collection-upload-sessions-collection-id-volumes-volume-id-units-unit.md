# PUT /v1/collection-upload-sessions/{collection_id}/volumes/{volume_id}/units/{unit}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:put-v1-collection-upload-sessions-collect-859266a156:ea12ca858c -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `collection-upload-sessions` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collection-upload-sessions~1{collection_id}~1volumes~1{volume_id}~1units~1{unit}/put`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Related interface records

- [Operation parity: put_collection_upload_session_unit](../operation/operation-parity-put-collection-upload-session-unit.md)

## Referenced contract dossiers

- [schemas: CollectionUploadUnitWorkDocument](schemas-collectionuploadunitworkdocument.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)

## Contract summary

- `operationId`: put_collection_upload_session_unit
- `summary`: Put Collection Upload Session Unit
- `security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| `collection_id` | path | yes | integer |
| `volume_id` | path | yes | string |
| `unit` | path | yes | integer |
| `If-Match` | header | yes | string |
| `Content-Length` | header | yes | integer |

### Request body

`{"content": {"application/octet-stream": {"schema": {"contentMediaType": "application/octet-stream", "format": "binary", "title": "Content", "type": "string"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| `200` | Successful Response |
| `400` | Bad Request |
| `401` | Unauthorized |
| `403` | Forbidden |
| `404` | Not Found |
| `409` | Conflict |
| `411` | Length Required |
| `500` | Internal Server Error |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bee866fb9f1ae678eaaa1e6d06eeb6d83778096d28cf742e850e6f73d99f405e -->

```json
{
  "operationId": "put_collection_upload_session_unit",
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
      "in": "path",
      "name": "volume_id",
      "required": true,
      "schema": {
        "pattern": "^(?:pack|segment)-[0-9a-f]{64}$",
        "title": "Volume Id",
        "type": "string"
      }
    },
    {
      "in": "path",
      "name": "unit",
      "required": true,
      "schema": {
        "minimum": 0,
        "title": "Unit",
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
    },
    {
      "description": "Exact request-body length in bytes.",
      "in": "header",
      "name": "Content-Length",
      "required": true,
      "schema": {
        "minimum": 0,
        "type": "integer"
      }
    }
  ],
  "requestBody": {
    "content": {
      "application/octet-stream": {
        "schema": {
          "contentMediaType": "application/octet-stream",
          "format": "binary",
          "title": "Content",
          "type": "string"
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
            "$ref": "#/components/schemas/CollectionUploadUnitWorkDocument"
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
    "411": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Length Required",
      "x-riverhog-error-codes": [
        "length_required"
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
  "summary": "Put Collection Upload Session Unit",
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
