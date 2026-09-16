# POST /v1/work/{work_id}/cancel

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:stove0:post-v1-work-work-id-cancel:19584813fe -->

Cancel Work

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-9fddf026dd"></a>
- <a id="s-99c239dfce"></a>`operationId`: cancel_work
- <a id="s-1d4623f18c"></a>`summary`: Cancel Work

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-e0d0a92d48"></a>`work_id` | path | yes | type="string" |

### Responses

| Status | Description |
|---|---|
| <a id="s-efcda5be1d"></a>`200` | Successful Response |
| <a id="s-def26bf134"></a>`400` | Bad Request |
| <a id="s-d95f5dbeee"></a>`401` | Unauthorized |
| <a id="s-f0bb7113fc"></a>`403` | Forbidden |
| <a id="s-f6f1cf0f50"></a>`404` | Not Found |
| <a id="s-58d8b67d6d"></a>`409` | Conflict |
| <a id="s-34eddfe3bb"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [stove0 work cancel](../../stove0-client/cli/stove0-work-cancel.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: WorkView](../http-schemas/schemas-workview.md)

## Governing policies

- <a id="pa-c15af5c8bb"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Structural operation bindings

This generated record links maintained client, CLI, response-authority, and provider routes. It checks interface structure, not executed qualification, successful CLI execution, or human/JSON equivalence. Test bindings and qualification commands are audit leads, not run results.

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "work cancel"
  ],
  "client": "Stove0ApiClient",
  "method": "POST",
  "operation_id": "cancel_work",
  "path": "/v1/work/{work_id}/cancel",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1work~1{work_id}~1cancel/post`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 709f86d867575f0ab61b173214de785fc5d70da88088e7fed5b7c5183c2553bd -->

```json
{
  "operationId": "cancel_work",
  "parameters": [
    {
      "in": "path",
      "name": "work_id",
      "required": true,
      "schema": {
        "title": "Work Id",
        "type": "string"
      }
    }
  ],
  "responses": {
    "200": {
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
  "summary": "Cancel Work",
  "tags": [
    "work"
  ]
}
```
