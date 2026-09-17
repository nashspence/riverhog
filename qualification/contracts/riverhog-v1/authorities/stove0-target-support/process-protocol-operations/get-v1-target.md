# GET /v1/target

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-operations:stove0-target-support:get-v1-target:7d126fb6a3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Process Protocol Operations](index.md) |

## External contract

<a id="s-cc04ad06d2"></a>
| Concern | Contract |
|---|---|
| <a id="s-b5386d0c54"></a>`error_schema` | `"ErrorResponse"` |
| <a id="s-40838b110c"></a>`errors` | `[{"code":"bad_request","status":400},{"code":"unauthorized","status":401},{"code":"target_failed","status":500}]` |
| <a id="s-1ac1a1cbe8"></a>`method` | `"GET"` |
| <a id="s-6922f0212f"></a>`path` | `"/v1/target"` |
| <a id="s-4c85079d5b"></a>`path_parameters` | `[]` |
| <a id="s-fa876b719f"></a>`request` | `{"kind":"none","schema":null}` |
| <a id="s-1f6e33f930"></a>`response` | `{"headers":[],"kind":"json","schema":"TargetContract","statuses":[200]}` |

## Maintained corroboration

### Related interface records

- [generated:stove0-target protocol](../process-protocol/generated-stove0-target-protocol.md)

## Governing policies

- <a id="pa-27ae111c49"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:stove0-target](../../../evidence/sources/authorities.md#src-2c42f9d39a) — [reference/stove0/packages/target-support/src/stove0\_target\_support/schemas.py::target\_schema\_bundle](../../../../../../reference/stove0/packages/target-support/src/stove0_target_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-target/http_binding/operations/0`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 579976aa0d1ddba83df73b67143ec2a50d48bb00eac909816300d63211b540e9 -->

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
      "code": "target_failed",
      "status": 500
    }
  ],
  "method": "GET",
  "path": "/v1/target",
  "path_parameters": [],
  "request": {
    "kind": "none",
    "schema": null
  },
  "response": {
    "headers": [],
    "kind": "json",
    "schema": "TargetContract",
    "statuses": [
      200
    ]
  }
}
```

</details>
