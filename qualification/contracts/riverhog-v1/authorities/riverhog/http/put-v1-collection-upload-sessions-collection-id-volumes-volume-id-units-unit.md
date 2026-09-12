# PUT /v1/collection-upload-sessions/{collection_id}/volumes/{volume_id}/units/{unit}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:put-v1-collection-upload-sessions-collect-859266a156:ea12ca858c -->

Put Collection Upload Session Unit

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [collection-upload-sessions](families/collection-upload-sessions/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-56fb5451d4"></a>
- <a id="s-59edab6dfe"></a>`operationId`: put_collection_upload_session_unit
- <a id="s-ebcc0b06a2"></a>`summary`: Put Collection Upload Session Unit
- <a id="s-18fe034da0"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-b098192ee1"></a>`collection_id` | path | yes | type="integer"; minimum=1 |
| <a id="s-b77d4eaf73"></a>`volume_id` | path | yes | type="string"; pattern="^(?:pack\|segment)-[0-9a-f]{64}$" |
| <a id="s-606a5c1e12"></a>`unit` | path | yes | type="integer"; minimum=0 |
| <a id="s-8408816c84"></a>`If-Match` | header | yes | type="string"; pattern="^\"[0-9a-f]{64}\"$" |
| <a id="s-4b9aa015e5"></a>`Content-Length` | header | yes | type="integer"; minimum=0 |

### <a id="s-2c66ce80f5"></a>Request body

`{"content": {"application/octet-stream": {"schema": {"contentMediaType": "application/octet-stream", "format": "binary", "title": "Content", "type": "string"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| <a id="s-6d38f9f84e"></a>`200` | Successful Response |
| <a id="s-2ccb8b683f"></a>`400` | Bad Request |
| <a id="s-cc97e447eb"></a>`401` | Unauthorized |
| <a id="s-f79b60e8a2"></a>`403` | Forbidden |
| <a id="s-0780afcadf"></a>`404` | Not Found |
| <a id="s-ba5f434727"></a>`409` | Conflict |
| <a id="s-2e46e1490b"></a>`411` | Length Required |
| <a id="s-1216d6dc27"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: put_collection_upload_session_unit](../operation/operation-parity-put-collection-upload-session-unit.md)

### Referenced contract dossiers

- [schemas: CollectionUploadUnitWorkDocument](schemas-collectionuploadunitworkdocument.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)

## Governing policies

- <a id="pa-601c292443"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collection-upload-sessions~1{collection_id}~1volumes~1{volume_id}~1units~1{unit}/put`

### Exact owned JSON

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
