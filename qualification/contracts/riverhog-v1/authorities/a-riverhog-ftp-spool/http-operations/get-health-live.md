# GET /health/live

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:a-riverhog-ftp-spool:get-health-live:440066afae -->

Health Live

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-74ef5a0407"></a>
- <a id="s-d23409247b"></a>`operationId`: `"ftp_spool_health_live"`
- <a id="s-6c933098ed"></a>`summary`: `"Health Live"`
- <a id="s-e07031c6f8"></a>`tags`: `["health"]`

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-2d55e87baa"></a>`200` | Successful Response | application/json | [HealthResponse](../http-schemas/schemas-healthresponse.md) | not declared |

## Maintained corroboration

### Related interface records

- [a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient.ftp_spool_health_live](../../a-riverhog-ftp-spool-client/python/a-riverhog-ftp-spool-client-riverhogftpspoolclient-ftp-spool-health-live.md)

### Referenced contract elements

- [schemas: HealthResponse](../http-schemas/schemas-healthresponse.md)

## Governing policies

- <a id="pa-66899beaa9"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:a-riverhog-ftp-spool](../../../evidence/sources/authorities.md#src-fdb5f95db7) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/app.py::create\_app.&lt;locals&gt;.health\_live](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/app.py#L243)

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
      "public_identity": "a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient.ftp_spool_health_live",
      "source": {
        "line": 80,
        "module": "a_riverhog_ftp_spool_client.client",
        "path": "some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/client.py",
        "symbol": "RiverhogFtpSpoolClient.ftp_spool_health_live"
      }
    }
  ],
  "method": "GET",
  "operation_id": "ftp_spool_health_live",
  "path": "/health/live",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/a-riverhog-ftp-spool/paths/~1health~1live/get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 84172652b1d1264e2dcab448a93b522bf015bdd06f0a51e1fa4d03612da64316 -->

```json
{
  "operationId": "ftp_spool_health_live",
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
