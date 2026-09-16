# POST /v1/archive/copies

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:post-v1-archive-copies:eb648e2ac7 -->

Create Or Resume Archive Copy

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-8df4a71950"></a>
- <a id="s-fabf2aad91"></a>`operationId`: create_or_resume_archive_copy
- <a id="s-ac95213ded"></a>`summary`: Create Or Resume Archive Copy
- <a id="s-b14612bf43"></a>`security`: `[{"HTTPBearer": []}]`

### <a id="s-556eabb798"></a>Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/CreateArchiveCopyRequest"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| <a id="s-39d8493e84"></a>`200` | Successful Response |
| <a id="s-b7745610eb"></a>`400` | Bad Request |
| <a id="s-ad6df6f94e"></a>`401` | Unauthorized |
| <a id="s-1ce05e5dcf"></a>`403` | Forbidden |
| <a id="s-b417964ab7"></a>`404` | Not Found |
| <a id="s-cc6d320143"></a>`409` | Conflict |
| <a id="s-3a188b5f67"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [piggity archive copy start](../../piggity/cli/piggity-archive-copy-start.md)

### Referenced contract dossiers

- [schemas: ArchiveCopyJobOut](../http-schemas/schemas-archivecopyjobout.md)
- [schemas: CreateArchiveCopyRequest](../http-schemas/schemas-createarchivecopyrequest.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)

## Governing policies

- <a id="pa-5590354b3b"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

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
    "archive copy start"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "create_or_resume_archive_copy",
  "path": "/v1/archive/copies",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```

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
