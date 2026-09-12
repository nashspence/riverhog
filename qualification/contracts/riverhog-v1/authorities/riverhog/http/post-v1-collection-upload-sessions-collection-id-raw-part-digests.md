# POST /v1/collection-upload-sessions/{collection_id}/raw-part-digests

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:post-v1-collection-upload-sessions-collec-fffabb89d2:3436e508f7 -->

Register Collection Upload Session Raw Part Digests

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [collection-upload-sessions](families/collection-upload-sessions/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-aee096c8acf4"></a>
- <a id="s-c5e60abce4d1"></a>`operationId`: register_collection_upload_session_raw_part_digests
- <a id="s-93c8c73dd721"></a>`summary`: Register Collection Upload Session Raw Part Digests
- <a id="s-2fbf3a332798"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-30fea773afa4"></a>`collection_id` | path | yes | type="integer"; minimum=1 |

### <a id="s-a616ac0e21e5"></a>Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/CollectionUploadRawDigestBatchDocument"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| <a id="s-17836e8b2c1c"></a>`200` | Successful Response |
| <a id="s-3459ae1e9e47"></a>`400` | Bad Request |
| <a id="s-d9bb97b0c32e"></a>`401` | Unauthorized |
| <a id="s-9db3fc314483"></a>`403` | Forbidden |
| <a id="s-b91b13003087"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: register_collection_upload_session_raw_part_digests](../operation/operation-parity-register-collection-upload-session-raw-part-digests.md)

### Referenced contract dossiers

- [schemas: CollectionUploadRawDigestBatchDocument](schemas-collectionuploadrawdigestbatchdocument.md)
- [schemas: CollectionUploadRawDigestProgressDocument](schemas-collectionuploadrawdigestprogressdocument.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)

## Governing policies

- <a id="pa-b7fab490cf44"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collection-upload-sessions~1{collection_id}~1raw-part-digests/post`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 527571e9a892d14a93459cea9c3b7f5805caf028d194be405932cfaa3b0961b4 -->

```json
{
  "operationId": "register_collection_upload_session_raw_part_digests",
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
          "$ref": "#/components/schemas/CollectionUploadRawDigestBatchDocument"
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
            "$ref": "#/components/schemas/CollectionUploadRawDigestProgressDocument"
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
  "summary": "Register Collection Upload Session Raw Part Digests",
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
