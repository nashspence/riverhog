# POST /v1/retrieval-jobs/{job_id}/renew

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:post-v1-retrieval-jobs-job-id-renew:ed4150bc59 -->

Renew Retrieval Job

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [retrieval-jobs](families/retrieval-jobs/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-6c2e444e5f"></a>
- <a id="s-b669235070"></a>`operationId`: renew_retrieval_job
- <a id="s-379eabb5c2"></a>`summary`: Renew Retrieval Job
- <a id="s-78681d061d"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-74e7089c43"></a>`job_id` | path | yes | type="string" |

### <a id="s-2acb4c40f4"></a>Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/RenewRetrievalJobRequest"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| <a id="s-c8bd4449c4"></a>`200` | Successful Response |
| <a id="s-250aa5dba8"></a>`400` | Bad Request |
| <a id="s-73b476ee43"></a>`401` | Unauthorized |
| <a id="s-6a633813c5"></a>`403` | Forbidden |
| <a id="s-7ea33057ee"></a>`404` | Not Found |
| <a id="s-9e23fed389"></a>`409` | Conflict |
| <a id="s-dcc971c3cd"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: renew_retrieval_job](../operation/operation-parity-renew-retrieval-job.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: RenewRetrievalJobRequest](schemas-renewretrievaljobrequest.md)
- [schemas: RetrievalJobOut](schemas-retrievaljobout.md)

## Governing policies

- <a id="pa-8bb168d1d6"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1retrieval-jobs~1{job_id}~1renew/post`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c79ee5cda02daedf7346985ee1f3efdc8bf099e36f619fe78e3f2e73bf499025 -->

```json
{
  "operationId": "renew_retrieval_job",
  "parameters": [
    {
      "in": "path",
      "name": "job_id",
      "required": true,
      "schema": {
        "title": "Job Id",
        "type": "string"
      }
    }
  ],
  "requestBody": {
    "content": {
      "application/json": {
        "schema": {
          "$ref": "#/components/schemas/RenewRetrievalJobRequest"
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
  "summary": "Renew Retrieval Job",
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
