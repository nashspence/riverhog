# POST /v1/objects/head

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-operations:riverhog-storage-adapter-support:post-v1-objects-head:cb280639b5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Operations](index.md) |

## External contract

<a id="s-0428c641b5"></a>
| Concern | Contract |
|---|---|
| <a id="s-f4181b24dc"></a>`error_schema` | `"StorageAdapterError"` |
| <a id="s-5ff11d0d20"></a>`errors` | `[{"code":"unauthorized","status":401},{"code":"invalid_request","status":400},{"code":"provider_unavailable","status":503},{"code":"internal_failure","status":500},{"code":"request_too_large","status":413},{"code":"invalid_path","status":400},{"code":"not_found","status":404},{"code":"identity_conflict","status":409},{"code":"integrity_failure","status":409}]` |
| <a id="s-dc6e2e4041"></a>`method` | `"POST"` |
| <a id="s-0816d5c9b5"></a>`path` | `"/v1/objects/head"` |
| <a id="s-722bc3063d"></a>`path_parameters` | `[]` |
| <a id="s-4a7bc15a45"></a>`request` | `{"kind":"json","schema":"ObjectHeadRequest"}` |
| <a id="s-b1b033233f"></a>`response` | `{"headers":[],"kind":"json","schema":"ObjectMetadataReceipt","statuses":[200]}` |

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

- <a id="pa-496b4b40b6"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/http_binding/operations/8`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 44a741c9a2602510de2469e2c01f26788ef20674ae1fd6bc8ee06be6342dbc2c -->

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
  "path": "/v1/objects/head",
  "path_parameters": [],
  "request": {
    "kind": "json",
    "schema": "ObjectHeadRequest"
  },
  "response": {
    "headers": [],
    "kind": "json",
    "schema": "ObjectMetadataReceipt",
    "statuses": [
      200
    ]
  }
}
```

</details>
