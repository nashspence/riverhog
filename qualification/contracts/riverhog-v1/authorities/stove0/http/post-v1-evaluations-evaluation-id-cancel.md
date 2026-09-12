# POST /v1/evaluations/{evaluation_id}/cancel

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:post-v1-evaluations-evaluation-id-cancel:1af9ea8fbb -->

Cancel Evaluation

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [evaluations](families/evaluations/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-8c50ecf718"></a>
- <a id="s-fd9b2cc2f1"></a>`operationId`: cancel_evaluation
- <a id="s-6dd46bc3f2"></a>`summary`: Cancel Evaluation

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-5863030673"></a>`evaluation_id` | path | yes | type="string" |

### Responses

| Status | Description |
|---|---|
| <a id="s-26caea921f"></a>`200` | Successful Response |
| <a id="s-5540b6256d"></a>`400` | Bad Request |
| <a id="s-175c74d432"></a>`401` | Unauthorized |
| <a id="s-ad4ed29f8c"></a>`403` | Forbidden |
| <a id="s-34fbffc57d"></a>`404` | Not Found |
| <a id="s-19f6481c95"></a>`409` | Conflict |
| <a id="s-a98b75a7ee"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: cancel_evaluation](../operation/operation-parity-cancel-evaluation.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: EvaluationView](schemas-evaluationview.md)

## Governing policies

- <a id="pa-cb9b17fe04"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1evaluations~1{evaluation_id}~1cancel/post`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 310033ec14c354651e86ae97eb54ba909ac3caec4b727edf1b736faae67467eb -->

```json
{
  "operationId": "cancel_evaluation",
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
  "summary": "Cancel Evaluation",
  "tags": [
    "evaluations"
  ]
}
```
