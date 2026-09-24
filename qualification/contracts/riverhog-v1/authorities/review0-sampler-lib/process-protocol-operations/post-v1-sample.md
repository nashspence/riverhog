# POST /v1/sample

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-operations:review0-sampler-lib:post-v1-sample:59d4da735f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-lib](../index.md) |
| Interface | [Process Protocol Operations](index.md) |

## External contract

<a id="s-fabb69fb87"></a>
| Concern | Contract |
|---|---|
| <a id="s-ccb5b0fe41"></a>`error_schema` | `"ErrorOut"` |
| <a id="s-3896d94b12"></a>`errors` | `[{"code":"invalid_sampler_request","status":400},{"code":"unauthorized","status":401},{"code":"sampler_changed","status":409},{"code":"request_too_large","status":413},{"code":"sampler_failed","status":500}]` |
| <a id="s-ffe2729598"></a>`method` | `"POST"` |
| <a id="s-25024c3b1b"></a>`path` | `"/v1/sample"` |
| <a id="s-6d8070ab07"></a>`path_parameters` | `[]` |
| <a id="s-064c090d35"></a>`request` | `{"kind":"json","schema":"SamplerRequest"}` |
| <a id="s-73d2d6046a"></a>`response` | `{"headers":[],"kind":"json","schema":"SamplerResult","statuses":[200]}` |

## Maintained corroboration

### Related interface records

- [generated:review0-sampler protocol](../process-protocol/generated-review0-sampler-protocol.md)

## Governing policies

- <a id="pa-d0df33979f"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:review0-sampler](../../../evidence/sources/authorities.md#src-8a54180841) — [some-implementations/stove0/review0/sampler/support/src/review0\_sampler\_lib/schemas.py::sampler\_schema\_bundle](../../../../../../some-implementations/stove0/review0/sampler/support/src/review0_sampler_lib/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:review0-sampler/http_binding/operations/1`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 33c521aff39308e554a00b22a6382ecd60d0788bbd5d6688b26b8b1eb943c2ef -->

```json
{
  "error_schema": "ErrorOut",
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
