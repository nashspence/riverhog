# GET /v1/observer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-operations:stove0-observer-support:get-v1-observer:b9909cb1fe -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Process Protocol Operations](index.md) |

## External contract

<a id="s-24d0534b7b"></a>
| Concern | Contract |
|---|---|
| <a id="s-9c307b2e97"></a>`error_schema` | `"ErrorOut"` |
| <a id="s-c571be7630"></a>`errors` | `[{"code":"bad_request","status":400},{"code":"unauthorized","status":401},{"code":"observer_failed","status":500}]` |
| <a id="s-ec203ee211"></a>`method` | `"GET"` |
| <a id="s-5b0c7365a1"></a>`path` | `"/v1/observer"` |
| <a id="s-291b6b2d14"></a>`path_parameters` | `[]` |
| <a id="s-c5a65efb59"></a>`request` | `{"kind":"none","schema":null}` |
| <a id="s-21d9f682cb"></a>`response` | `{"headers":[],"kind":"json","schema":"ObserverDescriptor","statuses":[200]}` |

## Maintained corroboration

### Related interface records

- [generated:stove0-observer protocol](../process-protocol/generated-stove0-observer-protocol.md)

## Governing policies

- <a id="pa-55b211d970"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:stove0-observer](../../../evidence/sources/authorities.md#src-dcc0b5485b) — [some-implementations/stove0/packages/observer-support/src/stove0\_observer\_support/schemas.py::observer\_schema\_bundle](../../../../../../some-implementations/stove0/packages/observer-support/src/stove0_observer_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-observer/http_binding/operations/0`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1a92949a304d06d4ada5a54e5adb1cb3b1760ba7f410e27abcf4d55af9696cdf -->

```json
{
  "error_schema": "ErrorOut",
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
      "code": "observer_failed",
      "status": 500
    }
  ],
  "method": "GET",
  "path": "/v1/observer",
  "path_parameters": [],
  "request": {
    "kind": "none",
    "schema": null
  },
  "response": {
    "headers": [],
    "kind": "json",
    "schema": "ObserverDescriptor",
    "statuses": [
      200
    ]
  }
}
```

</details>
