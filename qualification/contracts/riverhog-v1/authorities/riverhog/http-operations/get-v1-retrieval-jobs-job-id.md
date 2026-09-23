# GET /v1/retrieval-jobs/{job_id}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:get-v1-retrieval-jobs-job-id:8e6a677bcf -->

Get Retrieval Job

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-71ccd446bb"></a>
- <a id="s-9e1401aa96"></a>`operationId`: `"get_retrieval_job"`
- <a id="s-fe0300e289"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-60875a2877"></a>`summary`: `"Get Retrieval Job"`
- <a id="s-037dcae4a6"></a>`tags`: `["retrieval"]`
- <a id="s-141ea10b05"></a>`x-riverhog-interface`: `"client-only-primitive"`
- <a id="s-3305fbff0f"></a>`x-riverhog-permission-requirements`: `[{"any_of":["retrieval:manage"]}]`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-194d1e8490"></a>`job_id` | path | yes | not declared | type="string"; title="Job Id" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-cf9c7c61fe"></a>`200` | Successful Response | application/json | [RetrievalJobOut](../http-schemas/schemas-retrievaljobout.md) | not declared |
| <a id="s-72e083b274"></a>`400` | Bad Request | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `bad_request` |
| <a id="s-8d79726415"></a>`401` | Unauthorized | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `unauthorized` |
| <a id="s-93fcccb1fc"></a>`403` | Forbidden | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `forbidden` |
| <a id="s-475488a6c1"></a>`404` | Not Found | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `not_found` |
| <a id="s-7d0603b4c1"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |

## Maintained corroboration

### Related interface records

- [piggity local evict](../../piggity/cli/piggity-local-evict.md)
- [piggity local remove](../../piggity/cli/piggity-local-remove.md)
- [piggity local repair](../../piggity/cli/piggity-local-repair.md)
- [piggity local sync](../../piggity/cli/piggity-local-sync.md)
- [riverhog_client.ApiClient.get_retrieval_job](../../riverhog-client/python/riverhog-client-apiclient-get-retrieval-job.md)

### Referenced contract elements

- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: RetrievalJobOut](../http-schemas/schemas-retrievaljobout.md)

## Governing policies

- <a id="pa-e1d8d5571e"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [riverhog/src/riverhog\_api/routers/retrieval.py::get\_retrieval\_job](../../../../../../riverhog/src/riverhog_api/routers/retrieval.py#L289)

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
      "executable": "piggity",
      "result_identity": "piggity-cli-result/local/remove/v1",
      "source": {
        "line": 917,
        "module": "piggity.local",
        "path": "reference/riverhog/applications/piggity/src/piggity/local.py",
        "symbol": "remove_collection"
      }
    },
    {
      "command": "local sync",
      "executable": "piggity",
      "result_identity": "piggity-cli-result/local/sync/v1",
      "source": {
        "line": 1101,
        "module": "piggity.local",
        "path": "reference/riverhog/applications/piggity/src/piggity/local.py",
        "symbol": "sync"
      }
    },
    {
      "command": "local repair",
      "executable": "piggity",
      "result_identity": "piggity-cli-result/local/repair/v1",
      "source": {
        "line": 1122,
        "module": "piggity.local",
        "path": "reference/riverhog/applications/piggity/src/piggity/local.py",
        "symbol": "repair"
      }
    },
    {
      "command": "local evict",
      "executable": "piggity",
      "result_identity": "piggity-cli-result/local/evict/v1",
      "source": {
        "line": 1187,
        "module": "piggity.local",
        "path": "reference/riverhog/applications/piggity/src/piggity/local.py",
        "symbol": "evict"
      }
    }
  ],
  "cli_commands": [
    "local evict",
    "local remove",
    "local repair",
    "local sync"
  ],
  "client": "ApiClient",
  "client_bindings": [
    {
      "public_identity": "riverhog_client.ApiClient.get_retrieval_job",
      "source": {
        "line": 875,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.get_retrieval_job"
      }
    }
  ],
  "method": "GET",
  "operation_id": "get_retrieval_job",
  "path": "/v1/retrieval-jobs/{job_id}",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1retrieval-jobs~1{job_id}/get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: aade9c94885aca268a175cd3530202dbd9b6221fa2546dca2eefa14869d9dbde -->

```json
{
  "operationId": "get_retrieval_job",
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
  "summary": "Get Retrieval Job",
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
