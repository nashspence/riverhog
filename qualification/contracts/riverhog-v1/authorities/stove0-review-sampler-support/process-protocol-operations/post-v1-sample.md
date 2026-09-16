# POST /v1/sample

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-operations:stove0-review-sampler-support:post-v1-sample:957fe79e9a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-support](../index.md) |
| Interface | [Process Protocol Operations](index.md) |

## External contract

<a id="s-240a01383d"></a>
| Concern | Contract |
|---|---|
| <a id="s-afa8ec69d0"></a>`error_schema` | `"ErrorResponse"` |
| <a id="s-0e45809897"></a>`errors` | `[{"code":"invalid_sampler_request","status":400},{"code":"unauthorized","status":401},{"code":"sampler_changed","status":409},{"code":"request_too_large","status":413},{"code":"sampler_failed","status":500}]` |
| <a id="s-ffd3e30187"></a>`method` | `"POST"` |
| <a id="s-670f81c592"></a>`path` | `"/v1/sample"` |
| <a id="s-e2919ff254"></a>`path_parameters` | `[]` |
| <a id="s-244f2ace23"></a>`request` | `{"kind":"json","schema":"SamplerRequest"}` |
| <a id="s-20b1642177"></a>`response` | `{"headers":[],"kind":"json","schema":"SamplerResult","statuses":[200]}` |

## Maintained corroboration

### Related interface records

- [generated:stove0-review-sampler protocol](../process-protocol/generated-stove0-review-sampler-protocol.md)

## Governing policies

- <a id="pa-2c183400e8"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:stove0-review-sampler](../../../evidence/sources.md#src-b47f3f4d7b) — `reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/schemas.py::sampler_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-review-sampler/http_binding/operations/1`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1c0180c581eee21eabe97630b38aa4214136fddc48ce967ab4812ce73350cf33 -->

```json
{
  "error_schema": "ErrorResponse",
  "errors": [
    {
      "code": "invalid_sampler_request",
      "status": 400
    },
    {
      "code": "unauthorized",
      "status": 401
    },
    {
      "code": "sampler_changed",
      "status": 409
    },
    {
      "code": "request_too_large",
      "status": 413
    },
    {
      "code": "sampler_failed",
      "status": 500
    }
  ],
  "method": "POST",
  "path": "/v1/sample",
  "path_parameters": [],
  "request": {
    "kind": "json",
    "schema": "SamplerRequest"
  },
  "response": {
    "headers": [],
    "kind": "json",
    "schema": "SamplerResult",
    "statuses": [
      200
    ]
  }
}
```

</details>
