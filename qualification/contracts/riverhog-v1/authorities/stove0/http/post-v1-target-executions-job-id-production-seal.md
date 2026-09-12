# POST /v1/target-executions/{job_id}/production/seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:post-v1-target-executions-job-id-production-seal:3ea05ecc40 -->

Seal Target Execution Production

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [target-executions](families/target-executions/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-ce871e4a2a"></a>
- <a id="s-c91703d4ab"></a>`operationId`: seal_target_execution_production
- <a id="s-410182553f"></a>`summary`: Seal Target Execution Production

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-a6d0f47dfc"></a>`job_id` | path | yes | type="string" |

### Responses

| Status | Description |
|---|---|
| <a id="s-3dd5c8a6d0"></a>`200` | Successful Response |
| <a id="s-4d9f5a330c"></a>`400` | Bad Request |
| <a id="s-6acb6e5983"></a>`401` | Unauthorized |
| <a id="s-09302a55de"></a>`403` | Forbidden |
| <a id="s-ef82909d37"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: seal_target_execution_production](../operation/operation-parity-seal-target-execution-production.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: TargetProductionSealResponse](schemas-targetproductionsealresponse.md)

## Governing policies

- <a id="pa-a7f0afaabf"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1target-executions~1{job_id}~1production~1seal/post`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 108a3fc23fecc3ad541d1ed6137e217043ce16e228b47b2e6b571a63861727bd -->

```json
{
  "operationId": "seal_target_execution_production",
  "parameters": [
    {
      "in": "path",
      "name": "job_id",
      "required": true,
      "schema": {
        "title": "Job Id",
        "type": "string"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/TargetProductionSealResponse"
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
  "summary": "Seal Target Execution Production",
  "tags": [
    "target-executions"
  ],
  "x-riverhog-interface": "client-only-primitive"
}
```
