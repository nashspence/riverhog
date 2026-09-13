# GET /v1/collection-upload-sessions/{collection_id}/volumes/{volume_id}/units/{unit}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:get-v1-collection-upload-sessions-collect-5b1f74a3e2:a35a673033 -->

Get Collection Upload Session Unit

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-03e958c3fa"></a>
- <a id="s-10a9ba1965"></a>`operationId`: get_collection_upload_session_unit
- <a id="s-acf6897bf5"></a>`summary`: Get Collection Upload Session Unit
- <a id="s-e78eee3373"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-352d4ba0b7"></a>`collection_id` | path | yes | type="integer"; minimum=1 |
| <a id="s-849bd3e5fa"></a>`volume_id` | path | yes | type="string"; pattern="^(?:pack\|segment)-[0-9a-f]{64}$" |
| <a id="s-2dd410918c"></a>`unit` | path | yes | type="integer"; minimum=0 |

### Responses

| Status | Description |
|---|---|
| <a id="s-2b136e814e"></a>`200` | Successful Response |
| <a id="s-55aa3a0980"></a>`400` | Bad Request |
| <a id="s-eb39e3cb31"></a>`401` | Unauthorized |
| <a id="s-d05570bc11"></a>`403` | Forbidden |
| <a id="s-2adb6c1c8f"></a>`404` | Not Found |
| <a id="s-4f1f9d8690"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [piggity collection upload start](../../piggity/cli/piggity-collection-upload-start.md)

### Referenced contract dossiers

- [schemas: CollectionUploadUnitWorkDocument](../http-schemas/schemas-collectionuploadunitworkdocument.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)

## Governing policies

- <a id="pa-c375d71f12"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

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
  "method": "GET",
  "operation_id": "get_collection_upload_session_unit",
  "path": "/v1/collection-upload-sessions/{collection_id}/volumes/{volume_id}/units/{unit}",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "canonical-document"
}
```

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collection-upload-sessions~1{collection_id}~1volumes~1{volume_id}~1units~1{unit}/get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5a057d290b21843aa6e06281d489199fa5d11d40e4faaea00f7648bef049857d -->

```json
{
  "operationId": "get_collection_upload_session_unit",
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
    }
  ],
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
  "summary": "Get Collection Upload Session Unit",
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
