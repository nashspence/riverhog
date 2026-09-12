# POST /v1/archive/copies/retire

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:post-v1-archive-copies-retire:588158240c -->

Retire Archive Copy

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [archive](families/archive/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-d742ef1582"></a>
- <a id="s-ace6734f77"></a>`operationId`: retire_archive_copy
- <a id="s-f1554f94a5"></a>`summary`: Retire Archive Copy
- <a id="s-7d4d4809de"></a>`security`: `[{"HTTPBearer": []}]`

### <a id="s-c3063a6228"></a>Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/RetireArchiveCopyRequest"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| <a id="s-727ed61cf8"></a>`200` | Successful Response |
| <a id="s-1b8a617542"></a>`400` | Bad Request |
| <a id="s-2b4e80c9ee"></a>`401` | Unauthorized |
| <a id="s-4f887813da"></a>`403` | Forbidden |
| <a id="s-b4c84e0df8"></a>`404` | Not Found |
| <a id="s-13d5ed3af7"></a>`409` | Conflict |
| <a id="s-f26af1b3d1"></a>`500` | Internal Server Error |
| <a id="s-fc07f61e9d"></a>`503` | Service Unavailable |

## Maintained corroboration

### Related interface records

- [Operation parity: retire_archive_copy](../operation/operation-parity-retire-archive-copy.md)

### Referenced contract dossiers

- [schemas: ArchiveCopyRetirementResultOut](schemas-archivecopyretirementresultout.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: RetireArchiveCopyRequest](schemas-retirearchivecopyrequest.md)

## Governing policies

- <a id="pa-ce019d4bc8"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1archive~1copies~1retire/post`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 159685c11b3f5bdf37c5ebdd193be0bb4d5d736ccb37534c69abd59d282a0508 -->

```json
{
  "operationId": "retire_archive_copy",
  "requestBody": {
    "content": {
      "application/json": {
        "schema": {
          "$ref": "#/components/schemas/RetireArchiveCopyRequest"
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
            "$ref": "#/components/schemas/ArchiveCopyRetirementResultOut"
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
    },
    "503": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Service Unavailable",
      "x-riverhog-error-codes": [
        "service_unavailable"
      ]
    }
  },
  "security": [
    {
      "HTTPBearer": []
    }
  ],
  "summary": "Retire Archive Copy",
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
