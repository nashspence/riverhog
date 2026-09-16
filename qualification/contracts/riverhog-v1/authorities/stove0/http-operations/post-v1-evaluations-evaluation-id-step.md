# POST /v1/evaluations/{evaluation_id}/step

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:stove0:post-v1-evaluations-evaluation-id-step:6522b39189 -->

Step Evaluation

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-8664b755eb"></a>
- <a id="s-99d3b8b948"></a>`operationId`: `"step_evaluation"`
- <a id="s-1421731706"></a>`summary`: `"Step Evaluation"`
- <a id="s-c90182a44a"></a>`tags`: `["evaluations"]`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-bbfa65dac3"></a>`evaluation_id` | path | yes | not declared | type="string"; title="Evaluation Id" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-303b123a1e"></a>`200` | Successful Response | application/json | [EvaluationView](../http-schemas/schemas-evaluationview.md) | not declared |
| <a id="s-478745e81c"></a>`400` | Bad Request | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `bad_request` |
| <a id="s-5106019059"></a>`401` | Unauthorized | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `unauthorized` |
| <a id="s-49fa70a774"></a>`403` | Forbidden | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `forbidden` |
| <a id="s-a5023e9e73"></a>`404` | Not Found | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `not_found` |
| <a id="s-48f35a2280"></a>`409` | Conflict | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `conflict` |
| <a id="s-1e9f4e7e90"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |

## Maintained corroboration

### Related interface records

- [stove0 evaluation step](../../stove0-client/cli/stove0-evaluation-step.md)
- [stove0_api_client.Stove0ApiClient.step_evaluation](../../stove0-api-client/python/stove0-api-client-stove0apiclient-step-evaluation.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: EvaluationView](../http-schemas/schemas-evaluationview.md)

## Governing policies

- <a id="pa-f8cd6b7fdc"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **OpenAPI authority:** [openapi:stove0](../../../evidence/sources.md#src-52e6e32124)
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`
- **Handler:** [reference/stove0/application/server/src/stove0_api/app.py::create_app.<locals>.step_evaluation](../../../../../../reference/stove0/application/server/src/stove0_api/app.py#L890)

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
      "command": "evaluation step",
      "source": {
        "line": 430,
        "module": "stove0_cli.main",
        "path": "reference/stove0/application/client/src/stove0_cli/main.py",
        "symbol": "step_evaluation"
      }
    }
  ],
  "cli_commands": [
    "evaluation step"
  ],
  "client": "Stove0ApiClient",
  "client_bindings": [
    {
      "public_identity": "stove0_api_client.Stove0ApiClient.step_evaluation",
      "source": {
        "line": 403,
        "module": "stove0_api_client.client",
        "path": "reference/stove0/packages/api-client/src/stove0_api_client/client.py",
        "symbol": "Stove0ApiClient.step_evaluation"
      }
    }
  ],
  "method": "POST",
  "operation_id": "step_evaluation",
  "path": "/v1/evaluations/{evaluation_id}/step",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1evaluations~1{evaluation_id}~1step/post`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 920e77ffb89a2cced81cc10f787c5676b276cc690dfb9965b5fa317f3a589be6 -->

```json
{
  "operationId": "step_evaluation",
  "parameters": [
    {
      "in": "path",
      "name": "evaluation_id",
      "required": true,
      "schema": {
        "title": "Evaluation Id",
        "type": "string"
      }
    }
  ],
  "responses": {
    "200": {
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
  "summary": "Step Evaluation",
  "tags": [
    "evaluations"
  ]
}
```

</details>
