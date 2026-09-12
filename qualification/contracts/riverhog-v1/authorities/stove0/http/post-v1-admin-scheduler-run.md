# POST /v1/admin/scheduler/run

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:post-v1-admin-scheduler-run:05d3aa98a8 -->

Run Scheduler Once

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [admin](families/admin/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-07f314ca3b"></a>
- <a id="s-10624f0239"></a>`operationId`: run_scheduler
- <a id="s-ae0ebf4d08"></a>`summary`: Run Scheduler Once

### <a id="s-7f9f86f6c7"></a>Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/SchedulerRunIn"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| <a id="s-2c882565d9"></a>`200` | Successful Response |
| <a id="s-b4d23ae896"></a>`400` | Bad Request |
| <a id="s-57b6683d8a"></a>`401` | Unauthorized |
| <a id="s-88686cdefa"></a>`403` | Forbidden |
| <a id="s-5afdaa597f"></a>`409` | Conflict |
| <a id="s-91ff119ad2"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: run_scheduler](../operation/operation-parity-run-scheduler.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: SchedulerRun](schemas-schedulerrun.md)
- [schemas: SchedulerRunIn](schemas-schedulerrunin.md)

## Governing policies

- <a id="pa-78c76e268b"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1admin~1scheduler~1run/post`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0758c01c963012b849b3f3e9d46f68f930f3b19a774105f6a6f3dc31b20b7a6e -->

```json
{
  "operationId": "run_scheduler",
  "requestBody": {
    "content": {
      "application/json": {
        "schema": {
          "$ref": "#/components/schemas/SchedulerRunIn"
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
            "$ref": "#/components/schemas/SchedulerRun"
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
    "409": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Conflict",
      "x-riverhog-error-codes": [
        "conflict"
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
  "summary": "Run Scheduler Once",
  "tags": [
    "scheduler"
  ]
}
```
