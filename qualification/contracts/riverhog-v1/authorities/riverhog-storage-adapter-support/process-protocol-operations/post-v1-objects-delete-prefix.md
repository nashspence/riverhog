# POST /v1/objects/delete-prefix

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-operations:riverhog-storage-adapter-support:post-v1-objects-delete-prefix:a00aa2228a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Operations](index.md) |

## External contract

<a id="s-fe334fdda8"></a>
| Concern | Contract |
|---|---|
| <a id="s-f08a6cbc00"></a>`error_schema` | `"StorageAdapterError"` |
| <a id="s-f1c19ec040"></a>`errors` | `[{"code":"unauthorized","status":401},{"code":"invalid_request","status":400},{"code":"provider_unavailable","status":503},{"code":"internal_failure","status":500},{"code":"request_too_large","status":413},{"code":"invalid_path","status":400}]` |
| <a id="s-259caf2da3"></a>`method` | `"POST"` |
| <a id="s-c631825142"></a>`path` | `"/v1/objects/delete-prefix"` |
| <a id="s-f94dccf7d1"></a>`path_parameters` | `[]` |
| <a id="s-4cea6ce898"></a>`request` | `{"kind":"json","schema":"DeletePrefixRequest"}` |
| <a id="s-98a930f97e"></a>`response` | `{"headers":[],"kind":"json","schema":"MaintenanceResult","statuses":[200]}` |

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

- <a id="pa-2c37cc2699"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/http_binding/operations/11`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ae3100d22a295fb71f97c760a1e5eefd2bf9a680e557afb9c588d5450e031866 -->

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
    }
  ],
  "method": "POST",
  "path": "/v1/objects/delete-prefix",
  "path_parameters": [],
  "request": {
    "kind": "json",
    "schema": "DeletePrefixRequest"
  },
  "response": {
    "headers": [],
    "kind": "json",
    "schema": "MaintenanceResult",
    "statuses": [
      200
    ]
  }
}
```

</details>
