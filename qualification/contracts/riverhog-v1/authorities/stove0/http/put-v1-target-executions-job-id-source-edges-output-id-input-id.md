# PUT /v1/target-executions/{job_id}/source-edges/{output_id}/{input_id}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:put-v1-target-executions-job-id-source-ed-4cec468e6f:fbe1589e58 -->

Declare Target Execution Source Edge

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [target-executions](families/target-executions/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-aab8a01cb0a8"></a>
- <a id="s-b46c2b115c03"></a>`operationId`: declare_target_execution_source_edge
- <a id="s-3e190d97bb37"></a>`summary`: Declare Target Execution Source Edge

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-50bb08bc3470"></a>`job_id` | path | yes | type="string" |
| <a id="s-2b509a1ef3b9"></a>`output_id` | path | yes | type="string" |
| <a id="s-eb4dbcaa1001"></a>`input_id` | path | yes | type="string" |

### <a id="s-210a56550008"></a>Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/OutputSourceEdge"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| <a id="s-5bcc8cce4652"></a>`200` | Successful Response |
| <a id="s-019edde68f65"></a>`400` | Bad Request |
| <a id="s-ab9e47f964bc"></a>`401` | Unauthorized |
| <a id="s-eb5b519e3698"></a>`403` | Forbidden |
| <a id="s-fd7a8a0f4cdc"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: declare_target_execution_source_edge](../operation/operation-parity-declare-target-execution-source-edge.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: OutputSourceEdge](schemas-outputsourceedge.md)
- [schemas: TargetCallbackAcknowledgement](schemas-targetcallbackacknowledgement.md)

## Governing policies

- <a id="pa-93e579fd60ee"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1target-executions~1{job_id}~1source-edges~1{output_id}~1{input_id}/put`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: caf04471a028ef215ea47a144fd9f080e2d18e4033c5a8d5e428ad315e424fe8 -->

```json
{
  "operationId": "declare_target_execution_source_edge",
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
      "name": "output_id",
      "required": true,
      "schema": {
        "title": "Output Id",
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
          "$ref": "#/components/schemas/OutputSourceEdge"
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
  "summary": "Declare Target Execution Source Edge",
  "tags": [
    "target-executions"
  ],
  "x-riverhog-interface": "client-only-primitive"
}
```
