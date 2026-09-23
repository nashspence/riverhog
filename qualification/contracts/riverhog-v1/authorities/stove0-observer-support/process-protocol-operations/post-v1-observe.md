# POST /v1/observe

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-operations:stove0-observer-support:post-v1-observe:4bbc3f651c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Process Protocol Operations](index.md) |

## External contract

<a id="s-f0c398f5f6"></a>
| Concern | Contract |
|---|---|
| <a id="s-62bd1349a5"></a>`error_schema` | `"ErrorResponse"` |
| <a id="s-e5d2f7aede"></a>`errors` | `[{"code":"invalid_observation_request","status":400},{"code":"unauthorized","status":401},{"code":"request_too_large","status":413},{"code":"observer_failed","status":500}]` |
| <a id="s-d5ba2223c6"></a>`method` | `"POST"` |
| <a id="s-7c53238d9d"></a>`path` | `"/v1/observe"` |
| <a id="s-61601a5f52"></a>`path_parameters` | `[]` |
| <a id="s-68965b973b"></a>`request` | `{"kind":"json","schema":"ObservationInvocation"}` |
| <a id="s-baf226665e"></a>`response` | `{"headers":[],"kind":"json","schema":"ObservationResult","statuses":[200]}` |

## Maintained corroboration

### Related interface records

- [generated:stove0-observer protocol](../process-protocol/generated-stove0-observer-protocol.md)

## Governing policies

- <a id="pa-c52b7cb84b"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:stove0-observer](../../../evidence/sources/authorities.md#src-dcc0b5485b) — [some-implementations/stove0/packages/observer-support/src/stove0\_observer\_support/schemas.py::observer\_schema\_bundle](../../../../../../some-implementations/stove0/packages/observer-support/src/stove0_observer_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-observer/http_binding/operations/1`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 856e2af874532e9bb0137670df606d58b78e4976b3783287b5eebaed0ea3292f -->

```json
{
  "error_schema": "ErrorResponse",
  "errors": [
    {
      "code": "invalid_observation_request",
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
      "code": "observer_failed",
      "status": 500
    }
  ],
  "method": "POST",
  "path": "/v1/observe",
  "path_parameters": [],
  "request": {
    "kind": "json",
    "schema": "ObservationInvocation"
  },
  "response": {
    "headers": [],
    "kind": "json",
    "schema": "ObservationResult",
    "statuses": [
      200
    ]
  }
}
```

</details>
