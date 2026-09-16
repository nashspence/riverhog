# GET /v1/collection-upload-sessions/{collection_id}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:get-v1-collection-upload-sessions-collection-id:9ed76306c3 -->

Get Collection Upload Session

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-9877cbec4a"></a>
- <a id="s-d95219fd68"></a>`operationId`: get_collection_upload_session
- <a id="s-300fa0ab50"></a>`summary`: Get Collection Upload Session
- <a id="s-cf9f0e9198"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-656878bb3a"></a>`collection_id` | path | yes | type="integer"; minimum=1 |

### Responses

| Status | Description |
|---|---|
| <a id="s-3478a57d08"></a>`200` | Successful Response |
| <a id="s-a1e9012728"></a>`400` | Bad Request |
| <a id="s-7d44170c5b"></a>`401` | Unauthorized |
| <a id="s-43c4020b37"></a>`403` | Forbidden |
| <a id="s-11c9d10bc8"></a>`404` | Not Found |
| <a id="s-2d67e3ea29"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [piggity collection upload show](../../piggity/cli/piggity-collection-upload-show.md)
- [piggity collection upload start](../../piggity/cli/piggity-collection-upload-start.md)
- [piggity collection upload watch](../../piggity/cli/piggity-collection-upload-watch.md)

### Referenced contract dossiers

- [schemas: CollectionUploadSessionOut](../http-schemas/schemas-collectionuploadsessionout.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)

## Governing policies

- <a id="pa-11fa915fc3"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Structural operation bindings

This generated record links maintained client, CLI, response-authority, and provider routes. It checks interface structure, not executed qualification, successful CLI execution, or human/JSON equivalence. Test bindings and qualification commands are audit leads, not run results.

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "collection upload show",
    "collection upload start",
    "collection upload watch"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "get_collection_upload_session",
  "path": "/v1/collection-upload-sessions/{collection_id}",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collection-upload-sessions~1{collection_id}/get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b9001220f50a6c4fb52c4e0b9293f80d139faebfc46e13d46d393921c57fcaf3 -->

```json
{
  "operationId": "get_collection_upload_session",
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
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/CollectionUploadSessionOut"
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
  "security": [
    {
      "HTTPBearer": []
    }
  ],
  "summary": "Get Collection Upload Session",
  "tags": [
    "collections"
  ],
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "collections:create",
        "collections:delete"
      ]
    }
  ]
}
```
