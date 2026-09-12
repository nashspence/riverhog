# PUT /v1/target-executions/{job_id}/dispositions/{input_id}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:put-v1-target-executions-job-id-dispositi-608495d368:54f72585cb -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `target-executions` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1target-executions~1{job_id}~1dispositions~1{input_id}/put`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Related interface records

- [Operation parity: declare_target_execution_disposition](../operation/operation-parity-declare-target-execution-disposition.md)

## Referenced contract dossiers

- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: InputDispositionDeclaration](schemas-inputdispositiondeclaration.md)
- [schemas: TargetCallbackAcknowledgement](schemas-targetcallbackacknowledgement.md)

## Contract summary

- `operationId`: declare_target_execution_disposition
- `summary`: Declare Target Execution Disposition

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| `job_id` | path | yes | string |
| `input_id` | path | yes | string |

### Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/InputDispositionDeclaration"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| `200` | Successful Response |
| `400` | Bad Request |
| `401` | Unauthorized |
| `403` | Forbidden |
| `500` | Internal Server Error |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fd8022168ec6cf2ef70de7af26319d173e589ad80262dc4e8b61c2044ae714f7 -->

```json
{
  "operationId": "declare_target_execution_disposition",
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
      "in": "path",
      "name": "input_id",
      "required": true,
      "schema": {
        "title": "Input Id",
        "type": "string"
      }
    }
  ],
  "requestBody": {
    "content": {
      "application/json": {
        "schema": {
          "$ref": "#/components/schemas/InputDispositionDeclaration"
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
            "$ref": "#/components/schemas/TargetCallbackAcknowledgement"
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
  "summary": "Declare Target Execution Disposition",
  "tags": [
    "target-executions"
  ],
  "x-riverhog-interface": "client-only-primitive"
}
```
