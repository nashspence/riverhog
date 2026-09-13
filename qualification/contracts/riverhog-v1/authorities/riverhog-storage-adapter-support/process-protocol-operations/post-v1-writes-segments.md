# POST /v1/writes/segments

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-operations:riverhog-storage-adapter-support:post-v1-writes-segments:a12a148614 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Operations](index.md) |

## External contract

<a id="s-6d1962dea8"></a>
| Concern | Contract |
|---|---|
| <a id="s-5edef3c821"></a>`error_schema` | StorageAdapterError |
| <a id="s-81b1db5221"></a>`errors` | [{"code": "unauthorized", "status": 401}, {"code": "invalid_request", "status": 400}, {"code": "provider_unavailable", "status": 503}, {"code": "internal_failure", "status": 500}, {"code": "request_too_large", "status": 413}, {"code": "invalid_path", "status": 400}, {"code": "not_found", "status": 404}, {"code": "traversal_invalidated", "status": 409}] |
| <a id="s-534e3c1ec2"></a>`method` | POST |
| <a id="s-e4b1fa6f2a"></a>`path` | /v1/writes/segments |
| <a id="s-1612679c2f"></a>`path_parameters` | [] |
| <a id="s-09c7bb3666"></a>`request` | {"kind": "json", "schema": "WriteSegmentListRequest"} |
| <a id="s-e2a02ba80c"></a>`response` | {"headers": [], "kind": "json", "schema": "WriteSegmentPage", "statuses": [200]} |

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

- <a id="pa-6623a91905"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/http_binding/operations/3`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8ebf5f407282788f172398a1998e1d82a490bc98fba59b6be874fb6056e7e743 -->

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
      "code": "traversal_invalidated",
      "status": 409
    }
  ],
  "method": "POST",
  "path": "/v1/writes/segments",
  "path_parameters": [],
  "request": {
    "kind": "json",
    "schema": "WriteSegmentListRequest"
  },
  "response": {
    "headers": [],
    "kind": "json",
    "schema": "WriteSegmentPage",
    "statuses": [
      200
    ]
  }
}
```
