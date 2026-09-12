# POST /v1/collection-upload-sessions

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:post-v1-collection-upload-sessions:2ecff9ffc3 -->

Create Or Resume Collection Upload Session

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [collection-upload-sessions](families/collection-upload-sessions/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-179e7e60d9a9"></a>
- <a id="s-fff99c5076ef"></a>`operationId`: create_or_resume_collection_upload_session
- <a id="s-ad6863fb6091"></a>`summary`: Create Or Resume Collection Upload Session
- <a id="s-3c88b322fadf"></a>`security`: `[{"HTTPBearer": []}]`

### <a id="s-c4627822137d"></a>Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/CreateOrResumeCollectionUploadSessionRequest"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| <a id="s-3094ef177ac0"></a>`200` | Successful Response |
| <a id="s-7747b101f749"></a>`400` | Bad Request |
| <a id="s-5aabb4476daf"></a>`401` | Unauthorized |
| <a id="s-f6e118473b4f"></a>`403` | Forbidden |
| <a id="s-729c665c429d"></a>`409` | Conflict |
| <a id="s-e72adfdcf0c4"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: create_or_resume_collection_upload_session](../operation/operation-parity-create-or-resume-collection-upload-session.md)

### Referenced contract dossiers

- [schemas: CreateOrResumeCollectionUploadSessionOut](schemas-createorresumecollectionuploadsessionout.md)
- [schemas: CreateOrResumeCollectionUploadSessionRequest](schemas-createorresumecollectionuploadsessionrequest.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)

## Governing policies

- <a id="pa-3ff2ff7fc331"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collection-upload-sessions/post`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5a79f7f613f17b679d5f0a6e2a80b8a76ac5d4e18d28961712e5bafd1a5c115f -->

```json
{
  "operationId": "create_or_resume_collection_upload_session",
  "requestBody": {
    "content": {
      "application/json": {
        "schema": {
          "$ref": "#/components/schemas/CreateOrResumeCollectionUploadSessionRequest"
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
            "$ref": "#/components/schemas/CreateOrResumeCollectionUploadSessionOut"
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
  "summary": "Create Or Resume Collection Upload Session",
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
