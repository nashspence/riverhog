# POST /v1/retrieval-jobs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:post-v1-retrieval-jobs:e2da42a493 -->

Create Retrieval Job

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [retrieval-jobs](families/retrieval-jobs/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

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

- [Operation parity: create_retrieval_job](../operation/operation-parity-create-retrieval-job.md)

### Referenced contract dossiers

- [schemas: CreateRetrievalJobRequest](schemas-createretrievaljobrequest.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: RetrievalJobOut](schemas-retrievaljobout.md)

## Governing policies

- <a id="pa-b16edad571"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
