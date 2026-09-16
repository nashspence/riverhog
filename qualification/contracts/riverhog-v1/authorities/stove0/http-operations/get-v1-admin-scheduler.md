# GET /v1/admin/scheduler

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:stove0:get-v1-admin-scheduler:277013ce0b -->

Scheduler Status

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-ee6f210edd"></a>
- <a id="s-e826eae1b9"></a>`operationId`: `"scheduler_status"`
- <a id="s-04c2c6ac04"></a>`summary`: `"Scheduler Status"`
- <a id="s-087247a106"></a>`tags`: `["scheduler"]`

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-5842ab8baa"></a>`200` | Successful Response | application/json | [SchedulerStatus](../http-schemas/schemas-schedulerstatus.md) | not declared |
| <a id="s-4a9ebf1767"></a>`400` | Bad Request | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `bad_request` |
| <a id="s-1a0e304d13"></a>`401` | Unauthorized | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `unauthorized` |
| <a id="s-4cf0178ea8"></a>`403` | Forbidden | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `forbidden` |
| <a id="s-c304a4bbd8"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |

## Maintained corroboration

### Related interface records

- [stove0 scheduler status](../../stove0-client/cli/stove0-scheduler-status.md)
- [stove0_api_client.Stove0ApiClient.scheduler_status](../../stove0-api-client/python/stove0-api-client-stove0apiclient-scheduler-status.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: SchedulerStatus](../http-schemas/schemas-schedulerstatus.md)

## Governing policies

- <a id="pa-613b1abf60"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **OpenAPI authority:** [openapi:stove0](../../../evidence/sources.md#src-52e6e32124)
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`
- **Handler:** [reference/stove0/application/server/src/stove0_api/app.py::create_app.<locals>.scheduler_status](../../../../../../reference/stove0/application/server/src/stove0_api/app.py#L957)

### Structural operation bindings

This generated record links maintained client, CLI, response-authority, and provider routes. It checks interface structure, not executed qualification, successful CLI execution, or human/JSON equivalence. Test bindings and qualification commands are audit leads, not run results.

<details>
<summary>Exact structural binding record</summary>

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_bindings": [
    {
      "command": "scheduler status",
      "source": {
        "line": 489,
        "module": "stove0_cli.main",
        "path": "reference/stove0/application/client/src/stove0_cli/main.py",
        "symbol": "scheduler_status"
      }
    }
  ],
  "cli_commands": [
    "scheduler status"
  ],
  "client": "Stove0ApiClient",
  "client_bindings": [
    {
      "public_identity": "stove0_api_client.Stove0ApiClient.scheduler_status",
      "source": {
        "line": 448,
        "module": "stove0_api_client.client",
        "path": "reference/stove0/packages/api-client/src/stove0_api_client/client.py",
        "symbol": "Stove0ApiClient.scheduler_status"
      }
    }
  ],
  "method": "GET",
  "operation_id": "scheduler_status",
  "path": "/v1/admin/scheduler",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1admin~1scheduler/get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
