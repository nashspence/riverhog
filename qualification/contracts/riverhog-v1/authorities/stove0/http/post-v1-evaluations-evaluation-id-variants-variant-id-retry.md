# POST /v1/evaluations/{evaluation_id}/variants/{variant_id}/retry

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:post-v1-evaluations-evaluation-id-variant-ae6ffd6789:1f7e34dab4 -->

Retry Evaluation Variant

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [evaluations](families/evaluations/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-26a95a8b37"></a>
- <a id="s-e711229261"></a>`operationId`: retry_evaluation_variant
- <a id="s-5b6ee480cb"></a>`summary`: Retry Evaluation Variant

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-acc7d22fee"></a>`evaluation_id` | path | yes | type="string" |
| <a id="s-b1ce1f8680"></a>`variant_id` | path | yes | type="string" |

### Responses

| Status | Description |
|---|---|
| <a id="s-2718e2f1ce"></a>`200` | Successful Response |
| <a id="s-10349a25fd"></a>`400` | Bad Request |
| <a id="s-fcd2740ddf"></a>`401` | Unauthorized |
| <a id="s-e58243d5ed"></a>`403` | Forbidden |
| <a id="s-74f64068da"></a>`404` | Not Found |
| <a id="s-7899d0aac7"></a>`409` | Conflict |
| <a id="s-bba4f8f59e"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: retry_evaluation_variant](../operation/operation-parity-retry-evaluation-variant.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: EvaluationView](schemas-evaluationview.md)

## Governing policies

- <a id="pa-65dc9de010"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1evaluations~1{evaluation_id}~1variants~1{variant_id}~1retry/post`

### Exact owned JSON

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
