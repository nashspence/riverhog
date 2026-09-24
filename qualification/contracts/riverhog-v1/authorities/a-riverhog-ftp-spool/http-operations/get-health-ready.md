# GET /health/ready

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:a-riverhog-ftp-spool:get-health-ready:9dc818ae00 -->

Health Ready

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-54901c1936"></a>
- <a id="s-c9e80b38a3"></a>`operationId`: `"ftp_spool_health_ready"`
- <a id="s-644ff39fa1"></a>`summary`: `"Health Ready"`
- <a id="s-70f0641e2b"></a>`tags`: `["health"]`

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-5ec720d17c"></a>`200` | Successful Response | application/json | [HealthOut](../http-schemas/schemas-healthout.md) | not declared |
| <a id="s-7365e90bf3"></a>`503` | Service Unavailable | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | not declared |

## Maintained corroboration

### Related interface records

- [a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient.ftp_spool_health_ready](../../a-riverhog-ftp-spool-client/python/a-riverhog-ftp-spool-client-riverhogftpspoolclient-ftp-spool-health-ready.md)

### Referenced contract elements

- [schemas: ErrorOut](../http-schemas/schemas-errorout.md)
- [schemas: HealthOut](../http-schemas/schemas-healthout.md)

## Governing policies

- <a id="pa-008e54067d"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:a-riverhog-ftp-spool](../../../evidence/sources/authorities.md#src-fdb5f95db7) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/app.py::create\_app.&lt;locals&gt;.health\_ready](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/app.py#L252)

### Structural operation bindings

This generated record links maintained client, CLI, response-authority, and provider routes. It checks interface structure, not executed qualification, successful CLI execution, or human/JSON equivalence. Test bindings and qualification commands are audit leads, not run results.

<details>
<summary>Exact structural binding record</summary>

```json
{
  "application": "a-riverhog-ftp-spool",
  "classification": "standard-tool/protocol",
  "cli_bindings": [],
  "cli_commands": [],
  "client": "RiverhogFtpSpoolClient",
  "client_bindings": [
    {
      "public_identity": "a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient.ftp_spool_health_ready",
      "source": {
        "line": 83,
        "module": "a_riverhog_ftp_spool_client.client",
        "path": "some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/client.py",
        "symbol": "RiverhogFtpSpoolClient.ftp_spool_health_ready"
      }
    }
  ],
  "method": "GET",
  "operation_id": "ftp_spool_health_ready",
  "path": "/health/ready",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/a-riverhog-ftp-spool/paths/~1health~1ready/get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6c8313fe6360479663f4d2198360aaa3e889927670ffc30558995ef4de235814 -->

```json
{
  "operationId": "ftp_spool_health_ready",
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
    "503": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorOut"
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
