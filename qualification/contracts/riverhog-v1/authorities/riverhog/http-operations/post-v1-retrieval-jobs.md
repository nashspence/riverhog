# POST /v1/retrieval-jobs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:post-v1-retrieval-jobs:3fbd542396 -->

Create Retrieval Job

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-75436bdcc8"></a>
- <a id="s-1b2e0cfdae"></a>`operationId`: create_retrieval_job
- <a id="s-a09136103c"></a>`summary`: Create Retrieval Job
- <a id="s-78c10226e6"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-791d68906d"></a>`If-Match` | header | yes | type="string"; pattern="^\"[0-9a-f]{64}\"$" |

### <a id="s-b4debbc758"></a>Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/CreateRetrievalJobRequest"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| <a id="s-905ed75ccb"></a>`200` | Successful Response |
| <a id="s-ad1824e9e5"></a>`400` | Bad Request |
| <a id="s-9100a1ca8b"></a>`401` | Unauthorized |
| <a id="s-a9bd872e3a"></a>`403` | Forbidden |
| <a id="s-9b58850a83"></a>`404` | Not Found |
| <a id="s-63a5b41e29"></a>`409` | Conflict |
| <a id="s-f8eb2292f6"></a>`429` | Too Many Requests |
| <a id="s-973c8dc5f6"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [piggity local repair](../../piggity/cli/piggity-local-repair.md)
- [piggity local sync](../../piggity/cli/piggity-local-sync.md)

### Referenced contract dossiers

- [schemas: CreateRetrievalJobRequest](../http-schemas/schemas-createretrievaljobrequest.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: RetrievalJobOut](../http-schemas/schemas-retrievaljobout.md)

## Governing policies

- <a id="pa-b7768d4f1c"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

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
  "classification": "client-only-primitive",
  "cli_commands": [
    "local repair",
    "local sync"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "create_retrieval_job",
  "path": "/v1/retrieval-jobs",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1retrieval-jobs/post`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c01071e8ea388c976f80a3b29e85db51e3d191f0aecb3207abac1c6e3b689774 -->

```json
{
  "operationId": "create_retrieval_job",
  "parameters": [
    {
      "in": "header",
      "name": "If-Match",
      "required": true,
      "schema": {
        "pattern": "^\"[0-9a-f]{64}\"$",
        "title": "If-Match",
        "type": "string"
      }
    }
  ],
  "requestBody": {
    "content": {
      "application/json": {
        "schema": {
          "$ref": "#/components/schemas/CreateRetrievalJobRequest"
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
            "$ref": "#/components/schemas/RetrievalJobOut"
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
    "429": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Too Many Requests",
      "x-riverhog-error-codes": [
        "download_allowance_exceeded"
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
  "summary": "Create Retrieval Job",
  "tags": [
    "retrieval"
  ],
  "x-riverhog-interface": "client-only-primitive",
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "retrieval:manage"
      ]
    }
  ]
}
```
