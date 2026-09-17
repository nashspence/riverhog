# POST /v1/writes/completed

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-operations:riverhog-storage-adapter-support:post-v1-writes-completed:650889489d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Operations](index.md) |

## External contract

<a id="s-da5bdf3a18"></a>
| Concern | Contract |
|---|---|
| <a id="s-30cce58245"></a>`error_schema` | `"StorageAdapterError"` |
| <a id="s-1590a48a44"></a>`errors` | `[{"code":"unauthorized","status":401},{"code":"invalid_request","status":400},{"code":"provider_unavailable","status":503},{"code":"internal_failure","status":500},{"code":"request_too_large","status":413},{"code":"invalid_path","status":400},{"code":"not_found","status":404},{"code":"identity_conflict","status":409},{"code":"integrity_failure","status":409}]` |
| <a id="s-f8daa0056b"></a>`method` | `"POST"` |
| <a id="s-3d07eb2dbb"></a>`path` | `"/v1/writes/completed"` |
| <a id="s-6620a59987"></a>`path_parameters` | `[]` |
| <a id="s-99359536ef"></a>`request` | `{"kind":"json","schema":"CompletedWriteLookupRequest"}` |
| <a id="s-017ada70bc"></a>`response` | `{"headers":[],"kind":"json","schema":"CompletedObjectReceipt","statuses":[200]}` |

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

- <a id="pa-2bdd38e3df"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources/authorities.md#src-ef281f2471) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/schemas.py::storage\_adapter\_schema\_bundle](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/http_binding/operations/5`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 64f3d93ad30cbc27d0b912833e36fa5bf2f17baa3cfba1ecd0d13b6575e0a7d2 -->

```json
{
  "error_schema": "StorageAdapterError",
  "errors": [
    {
      "code": "unauthorized",
      "status": 401
    },
    {
      "code": "invalid_request",
      "status": 400
    },
    {
      "code": "provider_unavailable",
      "status": 503
    },
    {
      "code": "internal_failure",
      "status": 500
    },
    {
      "code": "request_too_large",
      "status": 413
    },
    {
      "code": "invalid_path",
      "status": 400
    },
    {
      "code": "not_found",
      "status": 404
    },
    {
      "code": "identity_conflict",
      "status": 409
    },
    {
      "code": "integrity_failure",
      "status": 409
    }
  ],
  "method": "POST",
  "path": "/v1/writes/completed",
  "path_parameters": [],
  "request": {
    "kind": "json",
    "schema": "CompletedWriteLookupRequest"
  },
  "response": {
    "headers": [],
    "kind": "json",
    "schema": "CompletedObjectReceipt",
    "statuses": [
      200
    ]
  }
}
```

</details>
