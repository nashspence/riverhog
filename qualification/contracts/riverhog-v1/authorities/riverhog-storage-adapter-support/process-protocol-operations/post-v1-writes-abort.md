# POST /v1/writes/abort

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-operations:riverhog-storage-adapter-support:post-v1-writes-abort:9aa9884f9c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Operations](index.md) |

## External contract

<a id="s-7700320470"></a>
| Concern | Contract |
|---|---|
| <a id="s-2a94458971"></a>`error_schema` | `"StorageAdapterError"` |
| <a id="s-57e8c870a3"></a>`errors` | `[{"code":"unauthorized","status":401},{"code":"invalid_request","status":400},{"code":"provider_unavailable","status":503},{"code":"internal_failure","status":500},{"code":"request_too_large","status":413},{"code":"invalid_path","status":400},{"code":"not_found","status":404}]` |
| <a id="s-fb68ee2d0a"></a>`method` | `"POST"` |
| <a id="s-5ecbdd917a"></a>`path` | `"/v1/writes/abort"` |
| <a id="s-b8ef0d7992"></a>`path_parameters` | `[]` |
| <a id="s-254b2a4eb1"></a>`request` | `{"kind":"json","schema":"WriteSession"}` |
| <a id="s-4854afe05b"></a>`response` | `{"headers":[],"kind":"none","schema":null,"statuses":[204]}` |

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

- <a id="pa-3e1bfd91c5"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/schemas.py::storage\_adapter\_schema\_bundle](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/http_binding/operations/6`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d1417c08cc90de0424fdba8635186b1121dc81ba8f084f1139c8b4a6f42bc673 -->

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
    }
  ],
  "method": "POST",
  "path": "/v1/writes/abort",
  "path_parameters": [],
  "request": {
    "kind": "json",
    "schema": "WriteSession"
  },
  "response": {
    "headers": [],
    "kind": "none",
    "schema": null,
    "statuses": [
      204
    ]
  }
}
```

</details>
