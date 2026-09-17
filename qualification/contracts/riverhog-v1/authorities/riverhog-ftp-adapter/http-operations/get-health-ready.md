# GET /health/ready

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog-ftp-adapter:get-health-ready:ac794d39e9 -->

Health Ready

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-46355a8b99"></a>
- <a id="s-07449a20f6"></a>`operationId`: `"ftp_adapter_health_ready"`
- <a id="s-d03f68c701"></a>`summary`: `"Health Ready"`
- <a id="s-f7aa15eb25"></a>`tags`: `["health"]`

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-6d25b467ef"></a>`200` | Successful Response | application/json | [HealthResponse](../http-schemas/schemas-healthresponse.md) | not declared |
| <a id="s-fb421d46a9"></a>`503` | Service Unavailable | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | not declared |

## Maintained corroboration

### Related interface records

- [riverhog_ftp_adapter_api_client.RiverhogFtpAdapterClient.ftp_adapter_health_ready](../../riverhog-ftp-adapter-api-client/python/riverhog-ftp-adapter-api-client-riverhogftpadapterclient-ftp-adapter-health-ready.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: HealthResponse](../http-schemas/schemas-healthresponse.md)

## Governing policies

- <a id="pa-af773e56fd"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog-ftp-adapter](../../../evidence/sources.md#src-c3a51ac29a) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [reference/riverhog/ingress/ftp/src/riverhog\_ftp\_adapter/app.py::create\_app.&lt;locals&gt;.health\_ready](../../../../../../reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/app.py#L252)

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
      "public_identity": "riverhog_ftp_adapter_api_client.RiverhogFtpAdapterClient.ftp_adapter_health_ready",
      "source": {
        "line": 83,
        "module": "riverhog_ftp_adapter_api_client.client",
        "path": "reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/client.py",
        "symbol": "RiverhogFtpAdapterClient.ftp_adapter_health_ready"
      }
    }
  ],
  "method": "GET",
  "operation_id": "ftp_adapter_health_ready",
  "path": "/health/ready",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog-ftp-adapter/paths/~1health~1ready/get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 29fd47a83019e36c295edfb3f7fddd8951196a3feaf23f6ce34cc043219d4bb9 -->

```json
{
  "operationId": "ftp_adapter_health_ready",
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
    "503": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Service Unavailable"
    }
  },
  "summary": "Health Ready",
  "tags": [
    "health"
  ]
}
```

</details>
