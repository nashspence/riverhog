# PUT /v1/target-executions/{job_id}/outputs/{artifact_id}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:put-v1-target-executions-job-id-outputs-artifact-id:c0b8267ea8 -->

Declare Target Execution Output

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [target-executions](families/target-executions/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-825ce221c4"></a>
- <a id="s-bc53930cb6"></a>`operationId`: declare_target_execution_output
- <a id="s-cd8891b235"></a>`summary`: Declare Target Execution Output

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-1dde977b0b"></a>`job_id` | path | yes | type="string" |
| <a id="s-f4d1dab401"></a>`artifact_id` | path | yes | type="string" |

### <a id="s-a90f312415"></a>Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/OutputArtifact"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| <a id="s-dd246f406c"></a>`200` | Successful Response |
| <a id="s-32354f6c91"></a>`400` | Bad Request |
| <a id="s-086336745e"></a>`401` | Unauthorized |
| <a id="s-b5701ceee7"></a>`403` | Forbidden |
| <a id="s-4f4a60a490"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: declare_target_execution_output](../operation/operation-parity-declare-target-execution-output.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: OutputArtifact](schemas-outputartifact.md)
- [schemas: TargetCallbackAcknowledgement](schemas-targetcallbackacknowledgement.md)

## Governing policies

- <a id="pa-e11fa4397b"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1target-executions~1{job_id}~1outputs~1{artifact_id}/put`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 54e3fa1eda84cc1c723219b1813ac10c24a1ee1b32c153fe97170e9e039d79dd -->

```json
{
  "operationId": "declare_target_execution_output",
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
      "name": "artifact_id",
      "required": true,
      "schema": {
        "title": "Artifact Id",
        "type": "string"
      }
    }
  ],
  "requestBody": {
    "content": {
      "application/json": {
        "schema": {
          "$ref": "#/components/schemas/OutputArtifact"
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
  "summary": "Declare Target Execution Output",
  "tags": [
    "target-executions"
  ],
  "x-riverhog-interface": "client-only-primitive"
}
```
