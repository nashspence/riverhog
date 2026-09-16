# POST /v1/evaluations/{evaluation_id}/variants/{variant_id}/retry

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:stove0:post-v1-evaluations-evaluation-id-variant-ae6ffd6789:56a329dcf3 -->

Retry Evaluation Variant

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-26a95a8b37"></a>
- <a id="s-e711229261"></a>`operationId`: `"retry_evaluation_variant"`
- <a id="s-5b6ee480cb"></a>`summary`: `"Retry Evaluation Variant"`
- <a id="s-182bed703f"></a>`tags`: `["evaluations"]`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-acc7d22fee"></a>`evaluation_id` | path | yes | not declared | type="string"; title="Evaluation Id" |
| <a id="s-b1ce1f8680"></a>`variant_id` | path | yes | not declared | type="string"; title="Variant Id" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-2718e2f1ce"></a>`200` | Successful Response | application/json | [EvaluationView](../http-schemas/schemas-evaluationview.md) | not declared |
| <a id="s-10349a25fd"></a>`400` | Bad Request | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `bad_request` |
| <a id="s-fcd2740ddf"></a>`401` | Unauthorized | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `unauthorized` |
| <a id="s-e58243d5ed"></a>`403` | Forbidden | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `forbidden` |
| <a id="s-74f64068da"></a>`404` | Not Found | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `not_found` |
| <a id="s-7899d0aac7"></a>`409` | Conflict | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `conflict` |
| <a id="s-bba4f8f59e"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |

## Maintained corroboration

### Related interface records

- [stove0 evaluation retry](../../stove0-client/cli/stove0-evaluation-retry.md)
- [stove0_api_client.Stove0ApiClient.retry_evaluation_variant](../../stove0-api-client/python/stove0-api-client-stove0apiclient-retry-evaluation-variant.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: EvaluationView](../http-schemas/schemas-evaluationview.md)

## Governing policies

- <a id="pa-99ee5f13ad"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **OpenAPI authority:** [openapi:stove0](../../../evidence/sources.md#src-52e6e32124)
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`
- **Handler:** [reference/stove0/application/server/src/stove0_api/app.py::create_app.<locals>.retry_evaluation_variant](../../../../../../reference/stove0/application/server/src/stove0_api/app.py#L920)

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
      "command": "evaluation retry",
      "source": {
        "line": 445,
        "module": "stove0_cli.main",
        "path": "reference/stove0/application/client/src/stove0_cli/main.py",
        "symbol": "retry_evaluation"
      }
    }
  ],
  "cli_commands": [
    "evaluation retry"
  ],
  "client": "Stove0ApiClient",
  "client_bindings": [
    {
      "public_identity": "stove0_api_client.Stove0ApiClient.retry_evaluation_variant",
      "source": {
        "line": 419,
        "module": "stove0_api_client.client",
        "path": "reference/stove0/packages/api-client/src/stove0_api_client/client.py",
        "symbol": "Stove0ApiClient.retry_evaluation_variant"
      }
    }
  ],
  "method": "POST",
  "operation_id": "retry_evaluation_variant",
  "path": "/v1/evaluations/{evaluation_id}/variants/{variant_id}/retry",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1evaluations~1{evaluation_id}~1variants~1{variant_id}~1retry/post`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3024fda285b6f3b045c801171d5a99813fc7483723ed3d087eff5964defd3d98 -->

```json
{
  "operationId": "retry_evaluation_variant",
  "parameters": [
    {
      "in": "path",
      "name": "evaluation_id",
      "required": true,
      "schema": {
        "title": "Evaluation Id",
        "type": "string"
      }
    },
    {
      "in": "path",
      "name": "variant_id",
      "required": true,
      "schema": {
        "title": "Variant Id",
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
  "summary": "Retry Evaluation Variant",
  "tags": [
    "evaluations"
  ]
}
```

</details>
