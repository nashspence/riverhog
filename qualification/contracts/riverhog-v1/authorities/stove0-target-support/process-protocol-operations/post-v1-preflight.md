# POST /v1/preflight

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-operations:stove0-target-support:post-v1-preflight:4cfc599ca3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Process Protocol Operations](index.md) |

## External contract

<a id="s-d8e30bacd5"></a>
| Concern | Contract |
|---|---|
| <a id="s-c865aaa678"></a>`error_schema` | `"ErrorOut"` |
| <a id="s-ad6fd2053f"></a>`errors` | `[{"code":"invalid_target_request","status":400},{"code":"unauthorized","status":401},{"code":"request_too_large","status":413},{"code":"target_protocol_mismatch","status":409},{"code":"operation_contract_mismatch","status":409},{"code":"unsupported_operation","status":400},{"code":"target_failed","status":500}]` |
| <a id="s-2eb9158922"></a>`method` | `"POST"` |
| <a id="s-53efa987ed"></a>`path` | `"/v1/preflight"` |
| <a id="s-190bb132dc"></a>`path_parameters` | `[]` |
| <a id="s-2ea0e5fe15"></a>`request` | `{"kind":"json","schema":"TargetPreflightRequest"}` |
| <a id="s-05bb75ce64"></a>`response` | `{"headers":[],"kind":"json","schema":"TargetPreflightResponse","statuses":[200]}` |

## Maintained corroboration

### Related interface records

- [generated:stove0-target protocol](../process-protocol/generated-stove0-target-protocol.md)

## Governing policies

- <a id="pa-46db2efb30"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:stove0-target](../../../evidence/sources/authorities.md#src-2c42f9d39a) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/schemas.py::target\_schema\_bundle](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-target/http_binding/operations/1`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0aacea71912e63a21a64d2c240deaee83ad93cc808f5a97614972a082b61824c -->

```json
{
  "error_schema": "ErrorOut",
  "errors": [
    {
      "code": "invalid_target_request",
      "status": 400
    },
    {
      "code": "unauthorized",
      "status": 401
    },
    {
      "code": "request_too_large",
      "status": 413
    },
    {
      "code": "target_protocol_mismatch",
      "status": 409
    },
    {
      "code": "operation_contract_mismatch",
      "status": 409
    },
    {
      "code": "unsupported_operation",
      "status": 400
    },
    {
      "code": "target_failed",
      "status": 500
    }
  ],
  "method": "POST",
  "path": "/v1/preflight",
  "path_parameters": [],
  "request": {
    "kind": "json",
    "schema": "TargetPreflightRequest"
  },
  "response": {
    "headers": [],
    "kind": "json",
    "schema": "TargetPreflightResponse",
    "statuses": [
      200
    ]
  }
}
```

</details>
