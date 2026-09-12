# POST /v1/archive/copies/retirement-plan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:post-v1-archive-copies-retirement-plan:ae0b94fc9b -->

Plan Archive Copy Retirement

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [archive](families/archive/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-36e9f6b51c"></a>
- <a id="s-3bfd8f184c"></a>`operationId`: plan_archive_copy_retirement
- <a id="s-50499b084e"></a>`summary`: Plan Archive Copy Retirement
- <a id="s-2b66881c40"></a>`security`: `[{"HTTPBearer": []}]`

### <a id="s-1af9509e17"></a>Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/ArchiveCopyRetirementRequest"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| <a id="s-75066bfe2a"></a>`200` | Successful Response |
| <a id="s-f96efd060b"></a>`400` | Bad Request |
| <a id="s-50ba041d14"></a>`401` | Unauthorized |
| <a id="s-338f4f888d"></a>`403` | Forbidden |
| <a id="s-1e1bb0b74b"></a>`404` | Not Found |
| <a id="s-c79f703bb8"></a>`409` | Conflict |
| <a id="s-a454625b80"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: plan_archive_copy_retirement](../operation/operation-parity-plan-archive-copy-retirement.md)

### Referenced contract dossiers

- [schemas: ArchiveCopyRetirementPlanOut](schemas-archivecopyretirementplanout.md)
- [schemas: ArchiveCopyRetirementRequest](schemas-archivecopyretirementrequest.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)

## Governing policies

- <a id="pa-70d4161d1a"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1archive~1copies~1retirement-plan/post`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7cd295c517a3009e8b3cd65f9e9ee8b3c55e78a3b3609d92dd0bb95df86528c9 -->

```json
{
  "operationId": "plan_archive_copy_retirement",
  "requestBody": {
    "content": {
      "application/json": {
        "schema": {
          "$ref": "#/components/schemas/ArchiveCopyRetirementRequest"
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
            "$ref": "#/components/schemas/ArchiveCopyRetirementPlanOut"
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
  "summary": "Plan Archive Copy Retirement",
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
