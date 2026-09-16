# GET /health/ready

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:stove0:get-health-ready:3a943dea39 -->

Health Ready

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-439e645d0c"></a>
- <a id="s-3ced2f8852"></a>`operationId`: health_ready
- <a id="s-f348037016"></a>`summary`: Health Ready

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-0d14b360c7"></a>`200` | Successful Response | application/json | [HealthResponse](../http-schemas/schemas-healthresponse.md) | not declared |
| <a id="s-238595f36d"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |
| <a id="s-79e0d02a26"></a>`503` | Service Unavailable | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `service_unavailable` |

## Maintained corroboration

### Related interface records

- [stove0 health](../../stove0-client/cli/stove0-health.md)
- [stove0_api_client.Stove0ApiClient.health_ready](../../stove0-api-client/python/stove0-api-client-stove0apiclient-health-ready.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: HealthResponse](../http-schemas/schemas-healthresponse.md)

## Governing policies

- <a id="pa-e6eb779028"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **OpenAPI authority:** [openapi:stove0](../../../evidence/sources.md#src-52e6e32124)
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`
- **Handler:** [reference/stove0/application/server/src/stove0_api/app.py::create_app.<locals>.health_ready](../../../../../../reference/stove0/application/server/src/stove0_api/app.py#L515)

### Structural operation bindings

This generated record links maintained client, CLI, response-authority, and provider routes. It checks interface structure, not executed qualification, successful CLI execution, or human/JSON equivalence. Test bindings and qualification commands are audit leads, not run results.

<details>
<summary>Exact structural binding record</summary>

```json
{
  "application": "stove0",
  "classification": "standard-tool/protocol",
  "cli_bindings": [
    {
      "command": "health",
      "source": {
        "line": 163,
        "module": "stove0_cli.main",
        "path": "reference/stove0/application/client/src/stove0_cli/main.py",
        "symbol": "health"
      }
    }
  ],
  "cli_commands": [
    "health"
  ],
  "client": "Stove0ApiClient",
  "client_bindings": [
    {
      "public_identity": "stove0_api_client.Stove0ApiClient.health_ready",
      "source": {
        "line": 136,
        "module": "stove0_api_client.client",
        "path": "reference/stove0/packages/api-client/src/stove0_api_client/client.py",
        "symbol": "Stove0ApiClient.health_ready"
      }
    }
  ],
  "method": "GET",
  "operation_id": "health_ready",
  "path": "/health/ready",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1health~1ready/get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 73735cc334d44871eee444468dca1ad3f7098d878b4d942201c85713ade1cee3 -->

```json
{
  "operationId": "health_ready",
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/HealthResponse"
          }
        }
      },
      "description": "Successful Response"
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
    },
    "503": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Service Unavailable",
      "x-riverhog-error-codes": [
        "service_unavailable"
      ]
    }
  },
  "summary": "Health Ready",
  "tags": [
    "health"
  ]
}
```

</details>
