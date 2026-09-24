# DELETE /v1/retrieval-jobs/{job_id}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:delete-v1-retrieval-jobs-job-id:290ffdbd74 -->

Cancel Retrieval Job

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-0896e23738"></a>
- <a id="s-b9ca215d6c"></a>`operationId`: `"cancel_retrieval_job"`
- <a id="s-1152f39421"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-0844e28a3f"></a>`summary`: `"Cancel Retrieval Job"`
- <a id="s-f591182c29"></a>`tags`: `["retrieval"]`
- <a id="s-352b6e0752"></a>`x-riverhog-interface`: `"client-only-primitive"`
- <a id="s-45ba757def"></a>`x-riverhog-permission-requirements`: `[{"any_of":["retrieval:manage"]}]`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-b087ed0d88"></a>`job_id` | path | yes | not declared | type="string"; title="Job Id" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-a7b97e3b89"></a>`200` | Successful Response | application/json | [RetrievalJobOut](../http-schemas/schemas-retrievaljobout.md) | not declared |
| <a id="s-2e70faf964"></a>`400` | Bad Request | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `bad_request` |
| <a id="s-eb5b5eaefb"></a>`401` | Unauthorized | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `unauthorized` |
| <a id="s-dc48191977"></a>`403` | Forbidden | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `forbidden` |
| <a id="s-c7bdef8a03"></a>`404` | Not Found | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `not_found` |
| <a id="s-750acddd3d"></a>`409` | Conflict | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `invalid_state` |
| <a id="s-7cfd168579"></a>`500` | Internal Server Error | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `internal_error` |

## Maintained corroboration

### Related interface records

- [a-riverhog-cli local evict](../../a-riverhog-cli/cli/a-riverhog-cli-local-evict.md)
- [a-riverhog-cli local remove](../../a-riverhog-cli/cli/a-riverhog-cli-local-remove.md)
- [riverhog_client.ApiClient.cancel_retrieval_job](../../riverhog-client/python/riverhog-client-apiclient-cancel-retrieval-job.md)

### Referenced contract elements

- [schemas: ErrorOut](../http-schemas/schemas-errorout.md)
- [schemas: RetrievalJobOut](../http-schemas/schemas-retrievaljobout.md)

## Governing policies

- <a id="pa-554fc131ce"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [riverhog/src/riverhog\_api/routers/retrieval.py::cancel\_retrieval\_job](../../../../../../riverhog/src/riverhog_api/routers/retrieval.py#L320)

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
      "command": "local remove",
      "executable": "a-riverhog-cli",
      "result_identity": "a-riverhog-cli-result/local/remove/v1",
      "source": {
        "line": 917,
        "module": "a_riverhog_cli.local",
        "path": "some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/local.py",
        "symbol": "remove_collection"
      }
    },
    {
      "command": "local evict",
      "executable": "a-riverhog-cli",
      "result_identity": "a-riverhog-cli-result/local/evict/v1",
      "source": {
        "line": 1189,
        "module": "a_riverhog_cli.local",
        "path": "some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/local.py",
        "symbol": "evict"
      }
    }
  ],
  "cli_commands": [
    "local evict",
    "local remove"
  ],
  "client": "ApiClient",
  "client_bindings": [
    {
      "public_identity": "riverhog_client.ApiClient.cancel_retrieval_job",
      "source": {
        "line": 889,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.cancel_retrieval_job"
      }
    }
  ],
  "method": "DELETE",
  "operation_id": "cancel_retrieval_job",
  "path": "/v1/retrieval-jobs/{job_id}",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1retrieval-jobs~1{job_id}/delete`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9c710f95f98ced3660ce9f1037164e78cbfe7d47fd1d10cf7ab1997885c0b246 -->

```json
{
  "operationId": "cancel_retrieval_job",
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
            "$ref": "#/components/schemas/ErrorOut"
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
            "$ref": "#/components/schemas/ErrorOut"
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
            "$ref": "#/components/schemas/ErrorOut"
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
            "$ref": "#/components/schemas/ErrorOut"
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
            "$ref": "#/components/schemas/ErrorOut"
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
            "$ref": "#/components/schemas/ErrorOut"
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
  "summary": "Cancel Retrieval Job",
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
