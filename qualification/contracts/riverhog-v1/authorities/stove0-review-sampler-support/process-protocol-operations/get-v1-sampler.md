# GET /v1/sampler

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-operations:stove0-review-sampler-support:get-v1-sampler:8b1a44dda9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-support](../index.md) |
| Interface | [Process Protocol Operations](index.md) |

## External contract

<a id="s-659220a2ef"></a>
| Concern | Contract |
|---|---|
| <a id="s-3aa2a51319"></a>`error_schema` | ErrorResponse |
| <a id="s-57144cd659"></a>`errors` | [{"code": "bad_request", "status": 400}, {"code": "unauthorized", "status": 401}, {"code": "sampler_failed", "status": 500}] |
| <a id="s-e553aeb86e"></a>`method` | GET |
| <a id="s-c4e84c5f96"></a>`path` | /v1/sampler |
| <a id="s-5d39065943"></a>`path_parameters` | [] |
| <a id="s-92e683f5f7"></a>`request` | {"kind": "none", "schema": null} |
| <a id="s-323597a61b"></a>`response` | {"headers": [], "kind": "json", "schema": "SamplerDescriptor", "statuses": [200]} |

## Maintained corroboration

### Related interface records

- [generated:stove0-review-sampler protocol](../process-protocol/generated-stove0-review-sampler-protocol.md)

## Governing policies

- <a id="pa-878aeec02a"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:stove0-review-sampler](../../../evidence/sources.md#src-b47f3f4d7b) — `reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/schemas.py::sampler_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-review-sampler/http_binding/operations/0`

### Exact owned JSON

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
