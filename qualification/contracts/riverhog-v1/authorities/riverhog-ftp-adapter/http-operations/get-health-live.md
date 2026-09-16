# GET /health/live

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog-ftp-adapter:get-health-live:c273434aab -->

Health Live

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-e0652acfec"></a>
- <a id="s-9b26680856"></a>`operationId`: `"ftp_adapter_health_live"`
- <a id="s-b44daa8478"></a>`summary`: `"Health Live"`
- <a id="s-75ab4de2f8"></a>`tags`: `["health"]`

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-ab22712980"></a>`200` | Successful Response | application/json | [HealthResponse](../http-schemas/schemas-healthresponse.md) | not declared |

## Maintained corroboration

### Related interface records

- [riverhog_ftp_adapter_api_client.RiverhogFtpAdapterClient.ftp_adapter_health_live](../../riverhog-ftp-adapter-api-client/python/riverhog-ftp-adapter-api-client-riverhogftpadapterclient-ftp-adapter-health-live.md)

### Referenced contract dossiers

- [schemas: HealthResponse](../http-schemas/schemas-healthresponse.md)

## Governing policies

- <a id="pa-69cc298c2e"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **OpenAPI authority:** [openapi:riverhog-ftp-adapter](../../../evidence/sources.md#src-c3a51ac29a)
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`
- **Handler:** [reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/app.py::create_app.<locals>.health_live](../../../../../../reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/app.py#L243)

### Structural operation bindings

This generated record links maintained client, CLI, response-authority, and provider routes. It checks interface structure, not executed qualification, successful CLI execution, or human/JSON equivalence. Test bindings and qualification commands are audit leads, not run results.

<details>
<summary>Exact structural binding record</summary>

```json
{
  "application": "riverhog-ftp-adapter",
  "classification": "standard-tool/protocol",
  "cli_bindings": [],
  "cli_commands": [],
  "client": "RiverhogFtpAdapterClient",
  "client_bindings": [
    {
      "public_identity": "riverhog_ftp_adapter_api_client.RiverhogFtpAdapterClient.ftp_adapter_health_live",
      "source": {
        "line": 80,
        "module": "riverhog_ftp_adapter_api_client.client",
        "path": "reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/client.py",
        "symbol": "RiverhogFtpAdapterClient.ftp_adapter_health_live"
      }
    }
  ],
  "method": "GET",
  "operation_id": "ftp_adapter_health_live",
  "path": "/health/live",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog-ftp-adapter/paths/~1health~1live/get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 648c33fcea1d5cfe6b1e4086907c6cf0758fac63142bd540eaced02c5ecdf6a2 -->

```json
{
  "operationId": "ftp_adapter_health_live",
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
    }
  },
  "summary": "Health Live",
  "tags": [
    "health"
  ]
}
```

</details>
