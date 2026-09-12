# GET /v1/target-executions/{job_id}/inputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:get-v1-target-executions-job-id-inputs:27650d0fa9 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `target-executions` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1target-executions~1{job_id}~1inputs/get`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/route-progression/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Related interface records

- [Operation parity: get_target_execution_inputs](../operation/operation-parity-get-target-execution-inputs.md)

## Referenced contract dossiers

- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: TargetInputPage](schemas-targetinputpage.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| logical-result-cardinality | items | `segmented_no_total_max` | reason=bounded-route-progression |

## Contract summary

- `operationId`: get_target_execution_inputs
- `summary`: Get Target Execution Inputs

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| `job_id` | path | yes | string |
| `continuation` | query | no | object (2 fields) |

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

<!-- exact-contract-value: 41d8c05b37cd6eaa1c9a042facbd01f171daf2c624193e40dbf9d662003e104f -->

```json
{
  "operationId": "get_target_execution_inputs",
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
      "in": "query",
      "name": "continuation",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "type": "string"
          },
          {
            "type": "null"
          }
        ],
        "title": "Continuation"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/TargetInputPage"
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
  "summary": "Get Target Execution Inputs",
  "tags": [
    "target-executions"
  ],
  "x-riverhog-interface": "client-only-primitive",
  "x-riverhog-read-collection": {
    "authority": "target-input-authority",
    "cursor_parameter": "continuation",
    "fixed_limit": 256,
    "kind": "exact-authority-page"
  }
}
```
