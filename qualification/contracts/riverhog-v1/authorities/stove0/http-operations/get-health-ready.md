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

| Status | Description |
|---|---|
| <a id="s-0d14b360c7"></a>`200` | Successful Response |
| <a id="s-238595f36d"></a>`500` | Internal Server Error |
| <a id="s-79e0d02a26"></a>`503` | Service Unavailable |

## Maintained corroboration

### Related interface records

- [stove0 health](../../stove0-client/cli/stove0-health.md)

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
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Structural operation bindings

This generated record links maintained client, CLI, response-authority, and provider routes. It checks interface structure, not executed qualification, successful CLI execution, or human/JSON equivalence. Test bindings and qualification commands are audit leads, not run results.

```json
{
  "application": "stove0",
  "classification": "standard-tool/protocol",
  "cli_commands": [
    "health"
  ],
  "client": "Stove0ApiClient",
  "method": "GET",
  "operation_id": "health_ready",
  "path": "/health/ready",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1health~1ready/get`

### Exact owned JSON

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
