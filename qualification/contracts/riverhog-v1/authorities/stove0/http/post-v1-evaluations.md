# POST /v1/evaluations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:post-v1-evaluations:0bc922d778 -->

Create Evaluation

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [evaluations](families/evaluations/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-cd6a7cc532"></a>
- <a id="s-2d04567f85"></a>`operationId`: create_evaluation
- <a id="s-69f2b2da84"></a>`summary`: Create Evaluation

### <a id="s-5f4b6ec140"></a>Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/EvaluationDefinition"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| <a id="s-1c5ea98d8a"></a>`201` | Successful Response |
| <a id="s-de7e63a5d5"></a>`400` | Bad Request |
| <a id="s-08f49ad156"></a>`401` | Unauthorized |
| <a id="s-d8276dc1ef"></a>`403` | Forbidden |
| <a id="s-80cd6bb9b4"></a>`409` | Conflict |
| <a id="s-823425572c"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: create_evaluation](../operation/operation-parity-create-evaluation.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: EvaluationDefinition](schemas-evaluationdefinition.md)
- [schemas: EvaluationView](schemas-evaluationview.md)

## Governing policies

- <a id="pa-402e6a8435"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1evaluations/post`

### Exact owned JSON

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
