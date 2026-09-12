# POST /v1/archive/copies

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:post-v1-archive-copies:d5b638b077 -->

Create Or Resume Archive Copy

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [archive](families/archive/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-8df4a7195088"></a>
- <a id="s-fabf2aad9113"></a>`operationId`: create_or_resume_archive_copy
- <a id="s-ac95213ded4d"></a>`summary`: Create Or Resume Archive Copy
- <a id="s-b14612bf434c"></a>`security`: `[{"HTTPBearer": []}]`

### <a id="s-556eabb798b9"></a>Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/CreateArchiveCopyRequest"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| <a id="s-39d8493e843d"></a>`200` | Successful Response |
| <a id="s-b7745610eb62"></a>`400` | Bad Request |
| <a id="s-ad6df6f94e60"></a>`401` | Unauthorized |
| <a id="s-1ce05e5dcf40"></a>`403` | Forbidden |
| <a id="s-b417964ab707"></a>`404` | Not Found |
| <a id="s-cc6d32014383"></a>`409` | Conflict |
| <a id="s-3a188b5f672b"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: create_or_resume_archive_copy](../operation/operation-parity-create-or-resume-archive-copy.md)

### Referenced contract dossiers

- [schemas: ArchiveCopyJobOut](schemas-archivecopyjobout.md)
- [schemas: CreateArchiveCopyRequest](schemas-createarchivecopyrequest.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)

## Governing policies

- <a id="pa-be0859655b93"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1archive~1copies/post`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d7580d6030a526c35862f4d9b363faba8b129bae0985fc4c53edc7fbd2fd06ee -->

```json
{
  "operationId": "create_or_resume_archive_copy",
  "requestBody": {
    "content": {
      "application/json": {
        "schema": {
          "$ref": "#/components/schemas/CreateArchiveCopyRequest"
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
            "$ref": "#/components/schemas/ArchiveCopyJobOut"
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
        "conflict",
        "invalid_state"
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
  "summary": "Create Or Resume Archive Copy",
  "tags": [
    "archive"
  ],
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "archives:manage"
      ]
    }
  ]
}
```
