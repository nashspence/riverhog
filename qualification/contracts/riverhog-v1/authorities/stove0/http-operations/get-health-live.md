# GET /health/live

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:stove0:get-health-live:4e078d336a -->

Health Live

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-fccde469e0"></a>
- <a id="s-b9f96c021d"></a>`operationId`: `"health_live"`
- <a id="s-56efa9f79c"></a>`summary`: `"Health Live"`
- <a id="s-57b3313fcb"></a>`tags`: `["health"]`

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-f426d91d27"></a>`200` | Successful Response | application/json | [HealthOut](../http-schemas/schemas-healthout.md) | not declared |
| <a id="s-76649245bd"></a>`500` | Internal Server Error | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `internal_error` |

## Maintained corroboration

### Related interface records

- [stove0 health](../../a-stove0-cli/cli/stove0-health.md)
- [stove0_api_client.Stove0ApiClient.health_live](../../stove0-api-client/python/stove0-api-client-stove0apiclient-health-live.md)

### Referenced contract elements

- [schemas: ErrorOut](../http-schemas/schemas-errorout.md)
- [schemas: HealthOut](../http-schemas/schemas-healthout.md)

## Governing policies

- <a id="pa-04205a7230"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [some-implementations/stove0/application/server/src/stove0\_api/app.py::create\_app.&lt;locals&gt;.health\_live](../../../../../../some-implementations/stove0/application/server/src/stove0_api/app.py#L508)

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
      "executable": "stove0",
      "result_identity": "stove0-cli-result/health/v1",
      "source": {
        "line": 163,
        "module": "a_stove0_cli.main",
        "path": "some-implementations/stove0/application/client/src/a_stove0_cli/main.py",
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
      "public_identity": "stove0_api_client.Stove0ApiClient.health_live",
      "source": {
        "line": 131,
        "module": "stove0_api_client.client",
        "path": "some-implementations/stove0/packages/api-client/src/stove0_api_client/client.py",
        "symbol": "Stove0ApiClient.health_live"
      }
    }
  ],
  "method": "GET",
  "operation_id": "health_live",
  "path": "/health/live",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1health~1live/get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 36015976e41436b957ef1cd7c5fd1816dfee8b739842a18962c9641a5d0e52c2 -->

```json
{
  "operationId": "health_live",
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/HealthOut"
          }
        }
      },
      "description": "Successful Response"
    },
    "500": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorOut"
          }
        }
      },
      "description": "Internal Server Error",
      "x-riverhog-error-codes": [
        "internal_error"
      ]
    }
  },
  "summary": "Health Live",
  "tags": [
    "health"
  ]
}
```

</details>
