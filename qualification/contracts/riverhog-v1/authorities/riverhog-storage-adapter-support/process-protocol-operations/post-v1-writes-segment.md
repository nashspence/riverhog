# POST /v1/writes/segment

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-operations:riverhog-storage-adapter-support:post-v1-writes-segment:eaa3da1373 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Operations](index.md) |

## External contract

<a id="s-03c1670982"></a>
| Concern | Contract |
|---|---|
| <a id="s-6f813e4ad5"></a>`error_schema` | `"StorageAdapterError"` |
| <a id="s-e17c32558d"></a>`errors` | `[{"code":"unauthorized","status":401},{"code":"invalid_request","status":400},{"code":"provider_unavailable","status":503},{"code":"internal_failure","status":500},{"code":"length_required","status":411},{"code":"request_too_large","status":413},{"code":"invalid_path","status":400},{"code":"not_found","status":404}]` |
| <a id="s-7e7663a8a5"></a>`method` | `"POST"` |
| <a id="s-fd5e55b5c5"></a>`path` | `"/v1/writes/segment"` |
| <a id="s-8a2723c538"></a>`path_parameters` | `[]` |
| <a id="s-5cd9c8363e"></a>`request` | `{"kind":"framed","schema":"WriteSegmentRequest"}` |
| <a id="s-4616d351cb"></a>`response` | `{"headers":[],"kind":"json","schema":"WriteSegmentReceipt","statuses":[200]}` |

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

- <a id="pa-88c8f6cf90"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/http_binding/operations/2`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4e54aabc3e5eebbc54f459dd968489d9c7256d6dfd2b68cd2ca1173bbdb5fb67 -->

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
      "code": "invalid_path",
      "status": 400
    },
    {
      "code": "not_found",
      "status": 404
    }
  ],
  "method": "POST",
  "path": "/v1/writes/segment",
  "path_parameters": [],
  "request": {
    "kind": "framed",
    "schema": "WriteSegmentRequest"
  },
  "response": {
    "headers": [],
    "kind": "json",
    "schema": "WriteSegmentReceipt",
    "statuses": [
      200
    ]
  }
}
```

</details>
