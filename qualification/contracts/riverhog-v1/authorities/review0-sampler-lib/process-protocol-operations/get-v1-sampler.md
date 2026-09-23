# GET /v1/sampler

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-operations:review0-sampler-lib:get-v1-sampler:b6e545bb5b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-lib](../index.md) |
| Interface | [Process Protocol Operations](index.md) |

## External contract

<a id="s-614080647b"></a>
| Concern | Contract |
|---|---|
| <a id="s-56222ca51e"></a>`error_schema` | `"ErrorResponse"` |
| <a id="s-5005df10cb"></a>`errors` | `[{"code":"bad_request","status":400},{"code":"unauthorized","status":401},{"code":"sampler_failed","status":500}]` |
| <a id="s-3da88f24e8"></a>`method` | `"GET"` |
| <a id="s-6dcae7397b"></a>`path` | `"/v1/sampler"` |
| <a id="s-1bab71411f"></a>`path_parameters` | `[]` |
| <a id="s-1b86f3a9e1"></a>`request` | `{"kind":"none","schema":null}` |
| <a id="s-a3cc53e209"></a>`response` | `{"headers":[],"kind":"json","schema":"SamplerDescriptor","statuses":[200]}` |

## Maintained corroboration

### Related interface records

- [generated:review0-sampler protocol](../process-protocol/generated-review0-sampler-protocol.md)

## Governing policies

- <a id="pa-6ea64e494e"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:review0-sampler](../../../evidence/sources/authorities.md#src-8a54180841) — [some-implementations/stove0/review0/sampler/support/src/review0\_sampler\_lib/schemas.py::sampler\_schema\_bundle](../../../../../../some-implementations/stove0/review0/sampler/support/src/review0_sampler_lib/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:review0-sampler/http_binding/operations/0`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bf61eb5b136c03e0a9478853194bda2cdd8b29bf1db1d42b02a6c630a19ba541 -->

```json
{
  "error_schema": "ErrorResponse",
  "errors": [
    {
      "code": "bad_request",
      "status": 400
    },
    {
      "code": "unauthorized",
      "status": 401
    },
    {
      "code": "sampler_failed",
      "status": 500
    }
  ],
  "method": "GET",
  "path": "/v1/sampler",
  "path_parameters": [],
  "request": {
    "kind": "none",
    "schema": null
  },
  "response": {
    "headers": [],
    "kind": "json",
    "schema": "SamplerDescriptor",
    "statuses": [
      200
    ]
  }
}
```

</details>
