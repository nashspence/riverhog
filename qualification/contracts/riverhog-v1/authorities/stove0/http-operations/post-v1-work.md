# POST /v1/work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:stove0:post-v1-work:9f500d8faf -->

Create Work

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Operations](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-4d80280945"></a>
- <a id="s-5d745b511d"></a>`operationId`: create_work
- <a id="s-9876a6f951"></a>`summary`: Create Work

### <a id="s-d1afd5577b"></a>Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/WorkCreateIn"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| <a id="s-bc8a3b692a"></a>`201` | Successful Response |
| <a id="s-91b5b5e2ec"></a>`400` | Bad Request |
| <a id="s-b690a7d530"></a>`401` | Unauthorized |
| <a id="s-0a5928f843"></a>`403` | Forbidden |
| <a id="s-563a5450db"></a>`404` | Not Found |
| <a id="s-cb81ab1c0b"></a>`409` | Conflict |
| <a id="s-3bdbaf0871"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [stove0-client work create](../../stove0-client/cli/stove0-client-work-create.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: WorkCreateIn](../http-schemas/schemas-workcreatein.md)
- [schemas: WorkView](../http-schemas/schemas-workview.md)

## Governing policies

- <a id="pa-523423cffe"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Operation qualification evidence

This evidence proves maintained client, CLI, response-authority, and provider qualification without creating a second semantic operation.

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "work create"
  ],
  "client": "Stove0ApiClient",
  "method": "POST",
  "operation_id": "create_work",
  "path": "/v1/work",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1work/post`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 75c389451a78879c7121549f8e50750e51abde916e90b5f2da1ff2c5b6e70714 -->

```json
{
  "operationId": "create_work",
  "requestBody": {
    "content": {
      "application/json": {
        "schema": {
          "$ref": "#/components/schemas/WorkCreateIn"
        }
      }
    },
    "required": true
  },
  "responses": {
    "201": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/WorkView"
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
  "summary": "Create Work",
  "tags": [
    "work"
  ]
}
```
