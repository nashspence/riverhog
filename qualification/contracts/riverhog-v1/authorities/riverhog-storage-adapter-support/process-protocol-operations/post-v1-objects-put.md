# POST /v1/objects/put

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-operations:riverhog-storage-adapter-support:post-v1-objects-put:40c016b604 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Operations](index.md) |

## External contract

<a id="s-8a27ba4698"></a>
| Concern | Contract |
|---|---|
| <a id="s-e5a1749671"></a>`error_schema` | `"StorageAdapterError"` |
| <a id="s-b3246d7079"></a>`errors` | `[{"code":"unauthorized","status":401},{"code":"invalid_request","status":400},{"code":"provider_unavailable","status":503},{"code":"internal_failure","status":500},{"code":"length_required","status":411},{"code":"request_too_large","status":413},{"code":"insufficient_storage","status":507},{"code":"invalid_path","status":400},{"code":"identity_conflict","status":409},{"code":"integrity_failure","status":409}]` |
| <a id="s-3ee4447850"></a>`method` | `"POST"` |
| <a id="s-0ccf5c36bc"></a>`path` | `"/v1/objects/put"` |
| <a id="s-393c0e70a9"></a>`path_parameters` | `[]` |
| <a id="s-c04dd2a955"></a>`request` | `{"kind":"framed","schema":"SmallObjectWriteRequest"}` |
| <a id="s-66607f7fc4"></a>`response` | `{"headers":[],"kind":"json","schema":"ImmutableObjectReceipt","statuses":[200]}` |

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

- <a id="pa-8b10d060a0"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/http_binding/operations/7`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ec17ce0ce17151befa817ac54b1cb5f89567b722e69c640e5c50df28b243a71c -->

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
      "code": "length_required",
      "status": 411
    },
    {
      "code": "request_too_large",
      "status": 413
    },
    {
      "code": "insufficient_storage",
      "status": 507
    },
    {
      "code": "invalid_path",
      "status": 400
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
  "path": "/v1/objects/put",
  "path_parameters": [],
  "request": {
    "kind": "framed",
    "schema": "SmallObjectWriteRequest"
  },
  "response": {
    "headers": [],
    "kind": "json",
    "schema": "ImmutableObjectReceipt",
    "statuses": [
      200
    ]
  }
}
```

</details>
