# GET /v1/work/{work_id}/coordination

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:get-v1-work-work-id-coordination:6149421462 -->

Inspect Work Coordination

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [work](families/work/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-cca302990f"></a>
- <a id="s-0e24b0e763"></a>`operationId`: inspect_work_coordination
- <a id="s-157d82e63a"></a>`summary`: Inspect Work Coordination

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-d01c651ce3"></a>`work_id` | path | yes | type="string" |

### Responses

| Status | Description |
|---|---|
| <a id="s-51b7e8e06d"></a>`200` | Successful Response |
| <a id="s-a4e0d2b42e"></a>`400` | Bad Request |
| <a id="s-0f87e73bae"></a>`401` | Unauthorized |
| <a id="s-9ae3130b0f"></a>`403` | Forbidden |
| <a id="s-2c0ea53089"></a>`404` | Not Found |
| <a id="s-da4f3c5f56"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: inspect_work_coordination](../operation/operation-parity-inspect-work-coordination.md)

### Referenced contract dossiers

- [schemas: BranchSetEvaluation](schemas-branchsetevaluation.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)

## Governing policies

- <a id="pa-88a0a44e73"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1work~1{work_id}~1coordination/get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: be6fc15455af78b7ea96b2d8c3cf5e5ca1f2ee410015542ba4a50e80a2c9815d -->

```json
{
  "operationId": "inspect_work_coordination",
  "parameters": [
    {
      "in": "path",
      "name": "work_id",
      "required": true,
      "schema": {
        "title": "Work Id",
        "type": "string"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/BranchSetEvaluation"
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
  "summary": "Inspect Work Coordination",
  "tags": [
    "work"
  ]
}
```
