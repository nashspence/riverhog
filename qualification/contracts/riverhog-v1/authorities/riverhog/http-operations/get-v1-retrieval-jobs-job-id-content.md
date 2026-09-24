# GET /v1/retrieval-jobs/{job_id}/content

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:get-v1-retrieval-jobs-job-id-content:f5f36a0875 -->

Download Retrieval File

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-c5492954bd"></a>
- <a id="s-5335e8d6ef"></a>`operationId`: `"download_retrieval_file"`
- <a id="s-8748e199d3"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-b38705bf02"></a>`summary`: `"Download Retrieval File"`
- <a id="s-f9ab1a72af"></a>`tags`: `["retrieval"]`
- <a id="s-81af2ee937"></a>`x-riverhog-interface`: `"client-only-primitive"`
- <a id="s-15dad46f05"></a>`x-riverhog-permission-requirements`: `[{"any_of":["retrieval:manage"]}]`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-4ac607f909"></a>`job_id` | path | yes | not declared | type="string"; title="Job Id" |
| <a id="s-0f44a5997d"></a>`collection_id` | query | yes | not declared | [CollectionIdParameter](../http-schemas/schemas-collectionidparameter.md) |
| <a id="s-c2dded5d77"></a>`path` | query | yes | not declared | type="string"; title="Path" |
| <a id="s-b26dd4f5ac"></a>`If-Match` | header | yes | not declared | type="string"; pattern="^\"[0-9a-f]{64}\"$"; title="If-Match" |
| <a id="s-2a974ee7d8"></a>`Range` | header | no | not declared | anyOf=[(type="string"); (type="null")]; title="Range" |
| <a id="s-a7a12d14ae"></a>`If-None-Match` | header | no | not declared | anyOf=[(type="string"); (type="null")]; title="If-None-Match" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-8aae1aafce"></a>`200` | Successful Response | — | not declared | not declared |
| <a id="s-96028b3e13"></a>`400` | Bad Request | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `bad_request` |
| <a id="s-8c8a839936"></a>`401` | Unauthorized | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `unauthorized` |
| <a id="s-34190f6e02"></a>`403` | Forbidden | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `forbidden` |
| <a id="s-ef41c9f9ca"></a>`404` | Not Found | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `not_found` |
| <a id="s-7fc74353a8"></a>`409` | Conflict | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `invalid_state` |
| <a id="s-eb7df68430"></a>`412` | Precondition Failed | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `precondition_failed` |
| <a id="s-fe427dd012"></a>`416` | Requested Range Not Satisfiable | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `invalid_range` |
| <a id="s-d380cee0b5"></a>`429` | Too Many Requests | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `download_allowance_exceeded` |
| <a id="s-fd7dcc0ebc"></a>`500` | Internal Server Error | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `internal_error` |

## Maintained corroboration

### Related interface records

- [a-riverhog-cli local repair](../../a-riverhog-cli/cli/a-riverhog-cli-local-repair.md)
- [a-riverhog-cli local sync](../../a-riverhog-cli/cli/a-riverhog-cli-local-sync.md)
- [riverhog_client.ApiClient.download_retrieval_file](../../riverhog-client/python/riverhog-client-apiclient-download-retrieval-file.md)

### Referenced contract elements

- [schemas: CollectionIdParameter](../http-schemas/schemas-collectionidparameter.md)
- [schemas: ErrorOut](../http-schemas/schemas-errorout.md)

## Governing policies

- <a id="pa-f9253e4522"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [riverhog/src/riverhog\_api/routers/retrieval.py::download\_retrieval\_file](../../../../../../riverhog/src/riverhog_api/routers/retrieval.py#L356)

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
      "public_identity": "riverhog_client.ApiClient.download_retrieval_file",
      "source": {
        "line": 980,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.download_retrieval_file"
      }
    }
  ],
  "method": "GET",
  "operation_id": "download_retrieval_file",
  "path": "/v1/retrieval-jobs/{job_id}/content",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "stream-or-empty"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1retrieval-jobs~1{job_id}~1content/get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1a6a8b83104c1ff78e268fa610823bd3f78c3cd3f9ccc0d31e761252c23d1c47 -->

```json
{
  "operationId": "download_retrieval_file",
  "parameters": [
    {
      "in": "path",
      "name": "job_id",
      "required": true,
      "schema": {
        "title": "Job Id",
        "type": "string"
      }
    },
    {
      "in": "query",
      "name": "collection_id",
      "required": true,
      "schema": {
        "$ref": "#/components/schemas/CollectionIdParameter"
      }
    },
    {
      "in": "query",
      "name": "path",
      "required": true,
      "schema": {
        "title": "Path",
        "type": "string"
      }
    },
    {
      "in": "header",
      "name": "If-Match",
      "required": true,
      "schema": {
        "pattern": "^\"[0-9a-f]{64}\"$",
        "title": "If-Match",
        "type": "string"
      }
    },
    {
      "in": "header",
      "name": "Range",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "type": "string"
          },
          {
            "type": "null"
          }
        ],
        "title": "Range"
      }
    },
    {
      "in": "header",
      "name": "If-None-Match",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "type": "string"
          },
          {
            "type": "null"
          }
        ],
        "title": "If-None-Match"
      }
    }
  ],
  "responses": {
    "200": {
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
    "412": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorOut"
          }
        }
      },
      "description": "Precondition Failed",
      "x-riverhog-error-codes": [
        "precondition_failed"
      ]
    },
    "416": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorOut"
          }
        }
      },
      "description": "Requested Range Not Satisfiable",
      "x-riverhog-error-codes": [
        "invalid_range"
      ]
    },
    "429": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorOut"
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
  "summary": "Download Retrieval File",
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
