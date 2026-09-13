# GET /v1/target-executions/{job_id}/inputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:stove0:get-v1-target-executions-job-id-inputs:6c7f3ca62d -->

Get Target Execution Inputs

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-dd498ea95c"></a>
- <a id="s-0754069f02"></a>`operationId`: get_target_execution_inputs
- <a id="s-8899a415e1"></a>`summary`: Get Target Execution Inputs

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-71bac0a8b3"></a>`job_id` | path | yes | type="string" |
| <a id="s-f3e43785ca"></a>`continuation` | query | no | anyOf=type="string" \| type="null" |

### Responses

| Status | Description |
|---|---|
| <a id="s-9070ff8872"></a>`200` | Successful Response |
| <a id="s-06d3c2e4e6"></a>`400` | Bad Request |
| <a id="s-7c20e23ee1"></a>`401` | Unauthorized |
| <a id="s-a3ab3c82cc"></a>`403` | Forbidden |
| <a id="s-f3a5b7a840"></a>`500` | Internal Server Error |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: progression={"authority":"target-input-authority","cursor_parameter":"continuation","fixed_limit":256,"kind":"exact-authority-page"}; reason="bounded-route-progression"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [GET /v1/target-executions/{job_id}/inputs](#s-dd498ea95c) | `logical-result-cardinality · items · segmented_no_total_max` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: TargetInputPage](../http-schemas/schemas-targetinputpage.md)

## Governing policies

- <a id="pa-23b18031cb"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-88288ccf0d"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Operation qualification evidence

This evidence proves maintained client, CLI, response-authority, and provider qualification without creating a second semantic operation.

```json
{
  "application": "stove0",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "TargetCallbackClient",
  "method": "GET",
  "operation_id": "get_target_execution_inputs",
  "path": "/v1/target-executions/{job_id}/inputs",
  "provider_evidence": null,
  "read_collection": {
    "authority": "target-input-authority",
    "cursor_parameter": "continuation",
    "fixed_limit": 256,
    "kind": "exact-authority-page"
  },
  "response_authority": "canonical-document"
}
```

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1target-executions~1{job_id}~1inputs/get`

### Exact owned JSON

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
