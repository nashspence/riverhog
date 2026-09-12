# POST /v1/evaluations/{evaluation_id}/step

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:post-v1-evaluations-evaluation-id-step:4ea0d6415c -->

Step Evaluation

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [evaluations](families/evaluations/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-8664b755eb"></a>
- <a id="s-99d3b8b948"></a>`operationId`: step_evaluation
- <a id="s-1421731706"></a>`summary`: Step Evaluation

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-bbfa65dac3"></a>`evaluation_id` | path | yes | type="string" |

### Responses

| Status | Description |
|---|---|
| <a id="s-303b123a1e"></a>`200` | Successful Response |
| <a id="s-478745e81c"></a>`400` | Bad Request |
| <a id="s-5106019059"></a>`401` | Unauthorized |
| <a id="s-49fa70a774"></a>`403` | Forbidden |
| <a id="s-a5023e9e73"></a>`404` | Not Found |
| <a id="s-48f35a2280"></a>`409` | Conflict |
| <a id="s-1e9f4e7e90"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: step_evaluation](../operation/operation-parity-step-evaluation.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: EvaluationView](schemas-evaluationview.md)

## Governing policies

- <a id="pa-c55e80231c"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1evaluations~1{evaluation_id}~1step/post`

### Exact owned JSON

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
