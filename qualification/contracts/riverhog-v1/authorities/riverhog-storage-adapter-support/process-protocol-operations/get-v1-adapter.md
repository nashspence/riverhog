# GET /v1/adapter

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-operations:riverhog-storage-adapter-support:get-v1-adapter:f132d75ec8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Operations](index.md) |

## External contract

<a id="s-2c73d5c0ad"></a>
| Concern | Contract |
|---|---|
| <a id="s-22e77dac9e"></a>`error_schema` | StorageAdapterError |
| <a id="s-f7afe900fd"></a>`errors` | [{"code": "unauthorized", "status": 401}, {"code": "invalid_request", "status": 400}, {"code": "provider_unavailable", "status": 503}, {"code": "internal_failure", "status": 500}] |
| <a id="s-219d2e0bb6"></a>`method` | GET |
| <a id="s-9fe9844dcd"></a>`path` | /v1/adapter |
| <a id="s-601e24df4b"></a>`path_parameters` | [] |
| <a id="s-e6b56028f0"></a>`request` | {"kind": "none", "schema": null} |
| <a id="s-fa956d8635"></a>`response` | {"headers": [], "kind": "json", "schema": "AdapterDescriptor", "statuses": [200]} |

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

- <a id="pa-4231f30e02"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/http_binding/operations/0`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f352e5d16e682d3a934dd23d03c69804d2a580a43295eeb727310aa9b0738988 -->

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
    }
  ],
  "method": "GET",
  "path": "/v1/adapter",
  "path_parameters": [],
  "request": {
    "kind": "none",
    "schema": null
  },
  "response": {
    "headers": [],
    "kind": "json",
    "schema": "AdapterDescriptor",
    "statuses": [
      200
    ]
  }
}
```
