# POST /v1/evaluations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:stove0:post-v1-evaluations:1a8ff20c1b -->

Create Evaluation

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-cd6a7cc532"></a>
- <a id="s-2d04567f85"></a>`operationId`: `"create_evaluation"`
- <a id="s-69f2b2da84"></a>`summary`: `"Create Evaluation"`
- <a id="s-985805b80e"></a>`tags`: `["evaluations"]`

### <a id="s-5f4b6ec140"></a>Request body

- `required`: `true`

| Media type | Schema |
|---|---|
| application/json | [EvaluationDefinition](../http-schemas/schemas-evaluationdefinition.md) |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-1c5ea98d8a"></a>`201` | Successful Response | application/json | [EvaluationView](../http-schemas/schemas-evaluationview.md) | not declared |
| <a id="s-de7e63a5d5"></a>`400` | Bad Request | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `bad_request` |
| <a id="s-08f49ad156"></a>`401` | Unauthorized | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `unauthorized` |
| <a id="s-d8276dc1ef"></a>`403` | Forbidden | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `forbidden` |
| <a id="s-80cd6bb9b4"></a>`409` | Conflict | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `conflict` |
| <a id="s-823425572c"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |

## Maintained corroboration

### Related interface records

- [stove0 evaluation create](../../a-stove0-cli/cli/stove0-evaluation-create.md)
- [stove0_api_client.Stove0ApiClient.create_evaluation](../../stove0-api-client/python/stove0-api-client-stove0apiclient-create-evaluation.md)

### Referenced contract elements

- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: EvaluationDefinition](../http-schemas/schemas-evaluationdefinition.md)
- [schemas: EvaluationView](../http-schemas/schemas-evaluationview.md)

## Governing policies

- <a id="pa-cc09a100e8"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [some-implementations/stove0/application/server/src/stove0\_api/app.py::create\_app.&lt;locals&gt;.create\_evaluation](../../../../../../some-implementations/stove0/application/server/src/stove0_api/app.py#L866)

### Structural operation bindings

This generated record links maintained client, CLI, response-authority, and provider routes. It checks interface structure, not executed qualification, successful CLI execution, or human/JSON equivalence. Test bindings and qualification commands are audit leads, not run results.

<details>
<summary>Exact structural binding record</summary>

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_bindings": [
    {
      "command": "evaluation create",
      "executable": "stove0",
      "result_identity": "stove0-cli-result/evaluation/create/v1",
      "source": {
        "line": 418,
        "module": "a_stove0_cli.main",
        "path": "some-implementations/stove0/application/client/src/a_stove0_cli/main.py",
        "symbol": "create_evaluation"
      }
    }
  ],
  "cli_commands": [
    "evaluation create"
  ],
  "client": "Stove0ApiClient",
  "client_bindings": [
    {
      "public_identity": "stove0_api_client.Stove0ApiClient.create_evaluation",
      "source": {
        "line": 387,
        "module": "stove0_api_client.client",
        "path": "some-implementations/stove0/packages/api-client/src/stove0_api_client/client.py",
        "symbol": "Stove0ApiClient.create_evaluation"
      }
    }
  ],
  "method": "POST",
  "operation_id": "create_evaluation",
  "path": "/v1/evaluations",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1evaluations/post`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 57adf3c879467cbc7b6fb28569c2f8c8fa93089a3f009141cb68b9d6c3f0b166 -->

```json
{
  "operationId": "create_evaluation",
  "requestBody": {
    "content": {
      "application/json": {
        "schema": {
          "$ref": "#/components/schemas/EvaluationDefinition"
        }
      }
    },
    "required": true
  },
  "responses": {
    "201": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/EvaluationView"
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
  "summary": "Create Evaluation",
  "tags": [
    "evaluations"
  ]
}
```

</details>
