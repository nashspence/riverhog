# POST /v1/target-executions/{job_id}/production/seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:stove0:post-v1-target-executions-job-id-production-seal:9c9d181598 -->

Seal Target Execution Production

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Operations](index.md) |

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

### Referenced contract dossiers

- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: TargetProductionSealResponse](../http-schemas/schemas-targetproductionsealresponse.md)

## Governing policies

- <a id="pa-6b63064986"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Structural operation bindings

This generated record links maintained client, CLI, response-authority, and provider routes. It checks interface structure, not executed qualification, successful CLI execution, or human/JSON equivalence. Test bindings and qualification commands are audit leads, not run results.

```json
{
  "application": "stove0",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "TargetCallbackClient",
  "method": "POST",
  "operation_id": "seal_target_execution_production",
  "path": "/v1/target-executions/{job_id}/production/seal",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```

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
