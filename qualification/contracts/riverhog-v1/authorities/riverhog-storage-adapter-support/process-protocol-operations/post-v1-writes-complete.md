# POST /v1/writes/complete

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-operations:riverhog-storage-adapter-support:post-v1-writes-complete:47b52f4419 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Operations](index.md) |

## External contract

<a id="s-00c79bf1ed"></a>
| Concern | Contract |
|---|---|
| <a id="s-30161acebf"></a>`error_schema` | `"StorageAdapterError"` |
| <a id="s-bc277f3c8a"></a>`errors` | `[{"code":"unauthorized","status":401},{"code":"invalid_request","status":400},{"code":"provider_unavailable","status":503},{"code":"internal_failure","status":500},{"code":"request_too_large","status":413},{"code":"invalid_path","status":400},{"code":"not_found","status":404},{"code":"identity_conflict","status":409},{"code":"integrity_failure","status":409}]` |
| <a id="s-d6aa095314"></a>`method` | `"POST"` |
| <a id="s-d9987c5c81"></a>`path` | `"/v1/writes/complete"` |
| <a id="s-3434a9570f"></a>`path_parameters` | `[]` |
| <a id="s-3a338164e9"></a>`request` | `{"kind":"json","schema":"WriteCompleteRequest"}` |
| <a id="s-e94ea7a570"></a>`response` | `{"headers":[],"kind":"json","schema":"CompletedObjectReceipt","statuses":[200]}` |

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

- <a id="pa-4836f06840"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/http_binding/operations/4`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6065488d2bc1baabb9b6ce09b54c5f2641d980dedf80584913d48def49b385d5 -->

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
  "path": "/v1/writes/complete",
  "path_parameters": [],
  "request": {
    "kind": "json",
    "schema": "WriteCompleteRequest"
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
