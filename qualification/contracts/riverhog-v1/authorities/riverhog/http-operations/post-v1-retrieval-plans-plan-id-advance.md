# POST /v1/retrieval-plans/{plan_id}/advance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:post-v1-retrieval-plans-plan-id-advance:89fe188d31 -->

Advance Retrieval Plan

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-eee1d15d4d"></a>
- <a id="s-3905b4ab13"></a>`operationId`: `"advance_retrieval_plan"`
- <a id="s-945eb7424f"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-81ae26728e"></a>`summary`: `"Advance Retrieval Plan"`
- <a id="s-bab491022f"></a>`tags`: `["retrieval"]`
- <a id="s-61dfb8c869"></a>`x-riverhog-interface`: `"client-only-primitive"`
- <a id="s-e21bc924e9"></a>`x-riverhog-permission-requirements`: `[{"any_of":["retrieval:manage"]}]`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-99944139c0"></a>`plan_id` | path | yes | not declared | type="string"; title="Plan Id" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-3fb020b34e"></a>`200` | Successful Response | application/json | [RetrievalPlanOut](../http-schemas/schemas-retrievalplanout.md) | not declared |
| <a id="s-a150a2ac0e"></a>`400` | Bad Request | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `bad_request` |
| <a id="s-95748db20a"></a>`401` | Unauthorized | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `unauthorized` |
| <a id="s-960c586011"></a>`403` | Forbidden | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `forbidden` |
| <a id="s-0ed376610d"></a>`404` | Not Found | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `not_found` |
| <a id="s-673acbfb18"></a>`500` | Internal Server Error | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `internal_error` |

## Maintained corroboration

### Related interface records

- [a-riverhog-cli local repair](../../a-riverhog-cli/cli/a-riverhog-cli-local-repair.md)
- [a-riverhog-cli local sync](../../a-riverhog-cli/cli/a-riverhog-cli-local-sync.md)
- [riverhog_client.ApiClient.advance_retrieval_plan](../../riverhog-client/python/riverhog-client-apiclient-advance-retrieval-plan.md)

### Referenced contract elements

- [schemas: ErrorOut](../http-schemas/schemas-errorout.md)
- [schemas: RetrievalPlanOut](../http-schemas/schemas-retrievalplanout.md)

## Governing policies

- <a id="pa-7b78f8f089"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [riverhog/src/riverhog\_api/routers/retrieval.py::advance\_retrieval\_plan](../../../../../../riverhog/src/riverhog_api/routers/retrieval.py#L205)

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
      "public_identity": "riverhog_client.ApiClient.advance_retrieval_plan",
      "source": {
        "line": 843,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.advance_retrieval_plan"
      }
    }
  ],
  "method": "POST",
  "operation_id": "advance_retrieval_plan",
  "path": "/v1/retrieval-plans/{plan_id}/advance",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1retrieval-plans~1{plan_id}~1advance/post`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8931d0b163447972ff4974cd4c6928ebec845e829c5279189d64ed2f6ce8d371 -->

```json
{
  "operationId": "advance_retrieval_plan",
  "parameters": [
    {
      "in": "path",
      "name": "plan_id",
      "required": true,
      "schema": {
        "title": "Plan Id",
        "type": "string"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/RetrievalPlanOut"
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
  "summary": "Advance Retrieval Plan",
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
