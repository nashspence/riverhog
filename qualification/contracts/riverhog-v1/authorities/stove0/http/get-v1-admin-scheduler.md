# GET /v1/admin/scheduler

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:get-v1-admin-scheduler:8cc1ec7c33 -->

Scheduler Status

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [admin](families/admin/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-ee6f210edd"></a>
- <a id="s-e826eae1b9"></a>`operationId`: scheduler_status
- <a id="s-04c2c6ac04"></a>`summary`: Scheduler Status

### Responses

| Status | Description |
|---|---|
| <a id="s-5842ab8baa"></a>`200` | Successful Response |
| <a id="s-4a9ebf1767"></a>`400` | Bad Request |
| <a id="s-1a0e304d13"></a>`401` | Unauthorized |
| <a id="s-4cf0178ea8"></a>`403` | Forbidden |
| <a id="s-c304a4bbd8"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: scheduler_status](../operation/operation-parity-scheduler-status.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: SchedulerStatus](schemas-schedulerstatus.md)

## Governing policies

- <a id="pa-fbbfe67ed7"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1admin~1scheduler/get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: df85822ac05825afbd0f11cab4672351ac1b9af2f6c721d413c89e046a2d0292 -->

```json
{
  "operationId": "scheduler_status",
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/SchedulerStatus"
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
  "summary": "Scheduler Status",
  "tags": [
    "scheduler"
  ]
}
```
