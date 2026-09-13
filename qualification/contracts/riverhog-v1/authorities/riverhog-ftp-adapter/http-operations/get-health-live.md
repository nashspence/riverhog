# GET /health/live

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog-ftp-adapter:get-health-live:c273434aab -->

Health Live

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [HTTP Operations](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-e0652acfec"></a>
- <a id="s-9b26680856"></a>`operationId`: ftp_adapter_health_live
- <a id="s-b44daa8478"></a>`summary`: Health Live

### Responses

| Status | Description |
|---|---|
| <a id="s-ab22712980"></a>`200` | Successful Response |

## Maintained corroboration

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
- [openapi:riverhog-ftp-adapter](../../../evidence/sources.md#src-c3a51ac29a) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Operation qualification evidence

This evidence proves maintained client, CLI, response-authority, and provider qualification without creating a second semantic operation.

```json
{
  "application": "riverhog-ftp-adapter",
  "classification": "standard-tool/protocol",
  "cli_commands": [],
  "client": "RiverhogFtpAdapterClient",
  "method": "GET",
  "operation_id": "ftp_adapter_health_live",
  "path": "/health/live",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```

### Machine authority

- `/external_contract/http_openapi/riverhog-ftp-adapter/paths/~1health~1live/get`

### Exact owned JSON

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
