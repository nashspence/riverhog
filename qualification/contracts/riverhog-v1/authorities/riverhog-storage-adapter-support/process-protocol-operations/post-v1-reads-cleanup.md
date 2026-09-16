# POST /v1/reads/cleanup

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-operations:riverhog-storage-adapter-support:post-v1-reads-cleanup:76538925c9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Operations](index.md) |

## External contract

<a id="s-2a66c41597"></a>
| Concern | Contract |
|---|---|
| <a id="s-eddc53602d"></a>`error_schema` | StorageAdapterError |
| <a id="s-52ec2a81c9"></a>`errors` | [{"code": "unauthorized", "status": 401}, {"code": "invalid_request", "status": 400}, {"code": "provider_unavailable", "status": 503}, {"code": "internal_failure", "status": 500}, {"code": "request_too_large", "status": 413}, {"code": "invalid_path", "status": 400}, {"code": "not_found", "status": 404}, {"code": "read_not_ready", "status": 409}, {"code": "read_expired", "status": 409}] |
| <a id="s-e2e5c63713"></a>`method` | POST |
| <a id="s-4c5fc8e9f1"></a>`path` | /v1/reads/cleanup |
| <a id="s-f2207a4e9f"></a>`path_parameters` | [] |
| <a id="s-8f502f6e2c"></a>`request` | {"kind": "json", "schema": "ReadPreparationRequest"} |
| <a id="s-e17f69b34e"></a>`response` | {"headers": [], "kind": "none", "schema": null, "statuses": [204]} |

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

- <a id="pa-552bc628ba"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/http_binding/operations/14`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1e2b603121dded48f5968e7a54bcb7ba3e670d6c9ff472c5be469b14188e88fb -->

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
      "code": "read_not_ready",
      "status": 409
    },
    {
      "code": "read_expired",
      "status": 409
    }
  ],
  "method": "POST",
  "path": "/v1/reads/cleanup",
  "path_parameters": [],
  "request": {
    "kind": "json",
    "schema": "ReadPreparationRequest"
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
