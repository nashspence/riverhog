# POST /v1/work/{work_id}/step

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:stove0:post-v1-work-work-id-step:7a179b3efe -->

Step Work

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-916d6fd99c"></a>
- <a id="s-1c5add23e0"></a>`operationId`: step_work
- <a id="s-7dbcf20582"></a>`summary`: Step Work

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-e4d2d64ba2"></a>`work_id` | path | yes | type="string" |

### Responses

| Status | Description |
|---|---|
| <a id="s-feaab7b4c6"></a>`200` | Successful Response |
| <a id="s-dba7bca4f9"></a>`400` | Bad Request |
| <a id="s-dfe4909773"></a>`401` | Unauthorized |
| <a id="s-568f6fab11"></a>`403` | Forbidden |
| <a id="s-9559591607"></a>`404` | Not Found |
| <a id="s-ea13fec2bd"></a>`409` | Conflict |
| <a id="s-00b91c8691"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [stove0-client work step](../../stove0-client/cli/stove0-client-work-step.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: WorkView](../http-schemas/schemas-workview.md)

## Governing policies

- <a id="pa-9874f5c929"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

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
    "work step"
  ],
  "client": "Stove0ApiClient",
  "method": "POST",
  "operation_id": "step_work",
  "path": "/v1/work/{work_id}/step",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1work~1{work_id}~1step/post`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7441c6b22fd59c005ffeb017618a3e2e213573f6ffb8571d4f5c44b8f8ff4104 -->

```json
{
  "operationId": "step_work",
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
  "summary": "Step Work",
  "tags": [
    "work"
  ]
}
```
