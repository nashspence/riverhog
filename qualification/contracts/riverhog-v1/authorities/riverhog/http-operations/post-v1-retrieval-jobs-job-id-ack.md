# POST /v1/retrieval-jobs/{job_id}/ack

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:post-v1-retrieval-jobs-job-id-ack:9c4da33f35 -->

Acknowledge Retrieval Job

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-822071ea62"></a>
- <a id="s-7b910fd0b0"></a>`operationId`: `"acknowledge_retrieval_job"`
- <a id="s-c5b18d23b9"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-4281b9e81b"></a>`summary`: `"Acknowledge Retrieval Job"`
- <a id="s-082dc8e4f2"></a>`tags`: `["retrieval"]`
- <a id="s-b563585573"></a>`x-riverhog-interface`: `"client-only-primitive"`
- <a id="s-8b4db762c9"></a>`x-riverhog-permission-requirements`: `[{"any_of":["retrieval:manage"]}]`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-51818aa473"></a>`job_id` | path | yes | not declared | type="string"; title="Job Id" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-e784886d8b"></a>`200` | Successful Response | application/json | [RetrievalJobOut](../http-schemas/schemas-retrievaljobout.md) | not declared |
| <a id="s-81b48c0e0a"></a>`400` | Bad Request | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `bad_request` |
| <a id="s-467ba4a933"></a>`401` | Unauthorized | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `unauthorized` |
| <a id="s-e65600e2d2"></a>`403` | Forbidden | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `forbidden` |
| <a id="s-5cfd96c51f"></a>`404` | Not Found | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `not_found` |
| <a id="s-e64eff21ca"></a>`409` | Conflict | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `invalid_state` |
| <a id="s-4331820c5c"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |

## Maintained corroboration

### Related interface records

- [a-riverhog-cli local repair](../../a-riverhog-cli/cli/a-riverhog-cli-local-repair.md)
- [a-riverhog-cli local sync](../../a-riverhog-cli/cli/a-riverhog-cli-local-sync.md)
- [riverhog_client.ApiClient.acknowledge_retrieval_job](../../riverhog-client/python/riverhog-client-apiclient-acknowledge-retrieval-job.md)

### Referenced contract elements

- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: RetrievalJobOut](../http-schemas/schemas-retrievaljobout.md)

## Governing policies

- <a id="pa-65f93996a6"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [riverhog/src/riverhog\_api/routers/retrieval.py::acknowledge\_retrieval\_job](../../../../../../riverhog/src/riverhog_api/routers/retrieval.py#L337)

### Structural operation bindings

This generated record links maintained client, CLI, response-authority, and provider routes. It checks interface structure, not executed qualification, successful CLI execution, or human/JSON equivalence. Test bindings and qualification commands are audit leads, not run results.

<details>
<summary>Exact structural binding record</summary>

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_bindings": [
    {
      "command": "local sync",
      "executable": "a-riverhog-cli",
      "result_identity": "a-riverhog-cli-result/local/sync/v1",
      "source": {
        "line": 1103,
        "module": "a_riverhog_cli.local",
        "path": "some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/local.py",
        "symbol": "sync"
      }
    },
    {
      "command": "local repair",
      "executable": "a-riverhog-cli",
      "result_identity": "a-riverhog-cli-result/local/repair/v1",
      "source": {
        "line": 1124,
        "module": "a_riverhog_cli.local",
        "path": "some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/local.py",
        "symbol": "repair"
      }
    }
  ],
  "cli_commands": [
    "local repair",
    "local sync"
  ],
  "client": "ApiClient",
  "client_bindings": [
    {
      "public_identity": "riverhog_client.ApiClient.acknowledge_retrieval_job",
      "source": {
        "line": 894,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.acknowledge_retrieval_job"
      }
    }
  ],
  "method": "POST",
  "operation_id": "acknowledge_retrieval_job",
  "path": "/v1/retrieval-jobs/{job_id}/ack",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1retrieval-jobs~1{job_id}~1ack/post`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7d4df7bde5634cc6914ad7554f68e8f6fd15e3c74cdef99838752b55cf651dae -->

```json
{
  "operationId": "acknowledge_retrieval_job",
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
  "summary": "Acknowledge Retrieval Job",
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

</details>
