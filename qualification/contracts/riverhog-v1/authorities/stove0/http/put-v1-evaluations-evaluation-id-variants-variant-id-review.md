# PUT /v1/evaluations/{evaluation_id}/variants/{variant_id}/review

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:put-v1-evaluations-evaluation-id-variants-a1782cc4d9:12e570fcf0 -->

Review Evaluation Variant

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [evaluations](families/evaluations/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-a59018dd23"></a>
- <a id="s-0cdb6ecda9"></a>`operationId`: review_evaluation_variant
- <a id="s-8cb5c2b984"></a>`summary`: Review Evaluation Variant

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-7d6d76bff4"></a>`evaluation_id` | path | yes | type="string" |
| <a id="s-d5d468b277"></a>`variant_id` | path | yes | type="string" |

### <a id="s-f2b96df34f"></a>Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/EvaluationReviewIn"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| <a id="s-efd76461f6"></a>`200` | Successful Response |
| <a id="s-a90a718e05"></a>`400` | Bad Request |
| <a id="s-3b25eb3bf7"></a>`401` | Unauthorized |
| <a id="s-7cd394e1c8"></a>`403` | Forbidden |
| <a id="s-009996cdec"></a>`404` | Not Found |
| <a id="s-34eb7fb837"></a>`409` | Conflict |
| <a id="s-3c3dc958bd"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: review_evaluation_variant](../operation/operation-parity-review-evaluation-variant.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: EvaluationReviewIn](schemas-evaluationreviewin.md)
- [schemas: EvaluationView](schemas-evaluationview.md)

## Governing policies

- <a id="pa-9dad7419fb"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1evaluations~1{evaluation_id}~1variants~1{variant_id}~1review/put`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fb16c91f67e058ffebb868f3416e277e583e882a6a7019fdb174e50e562b4d13 -->

```json
{
  "operationId": "review_evaluation_variant",
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
  "requestBody": {
    "content": {
      "application/json": {
        "schema": {
          "$ref": "#/components/schemas/EvaluationReviewIn"
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
  "summary": "Review Evaluation Variant",
  "tags": [
    "evaluations"
  ]
}
```
