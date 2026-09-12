# PUT /v1/target-executions/{job_id}/dispositions/{input_id}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:put-v1-target-executions-job-id-dispositi-608495d368:54f72585cb -->

Declare Target Execution Disposition

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [target-executions](families/target-executions/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-cd0c980798"></a>
- <a id="s-dcc45a72b8"></a>`operationId`: declare_target_execution_disposition
- <a id="s-b325b56157"></a>`summary`: Declare Target Execution Disposition

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-3770379334"></a>`job_id` | path | yes | type="string" |
| <a id="s-738b7756eb"></a>`input_id` | path | yes | type="string" |

### <a id="s-4af8a05da7"></a>Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/InputDispositionDeclaration"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| <a id="s-a5ce55528a"></a>`200` | Successful Response |
| <a id="s-2baee2a5b5"></a>`400` | Bad Request |
| <a id="s-5f90d24b53"></a>`401` | Unauthorized |
| <a id="s-7737fe8871"></a>`403` | Forbidden |
| <a id="s-ccb3dc68bd"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: declare_target_execution_disposition](../operation/operation-parity-declare-target-execution-disposition.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: InputDispositionDeclaration](schemas-inputdispositiondeclaration.md)
- [schemas: TargetCallbackAcknowledgement](schemas-targetcallbackacknowledgement.md)

## Governing policies

- <a id="pa-04eda13493"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1target-executions~1{job_id}~1dispositions~1{input_id}/put`

### Exact owned JSON

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
