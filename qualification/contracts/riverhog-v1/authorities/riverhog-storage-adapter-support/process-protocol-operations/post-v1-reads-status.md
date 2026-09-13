# POST /v1/reads/status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-operations:riverhog-storage-adapter-support:post-v1-reads-status:ce9c30ee67 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Operations](index.md) |

## External contract

<a id="s-7be148ce05"></a>
| Concern | Contract |
|---|---|
| <a id="s-cbe387acde"></a>`error_schema` | StorageAdapterError |
| <a id="s-985e18c430"></a>`errors` | [{"code": "unauthorized", "status": 401}, {"code": "invalid_request", "status": 400}, {"code": "provider_unavailable", "status": 503}, {"code": "internal_failure", "status": 500}, {"code": "request_too_large", "status": 413}, {"code": "invalid_path", "status": 400}, {"code": "not_found", "status": 404}, {"code": "read_not_ready", "status": 409}, {"code": "read_expired", "status": 409}] |
| <a id="s-f2d9b303bb"></a>`method` | POST |
| <a id="s-c5bc9a2809"></a>`path` | /v1/reads/status |
| <a id="s-8a6f7aa750"></a>`path_parameters` | [] |
| <a id="s-7549c94846"></a>`request` | {"kind": "json", "schema": "ReadPreparationRequest"} |
| <a id="s-615ba14ffd"></a>`response` | {"headers": [], "kind": "json", "schema": "ReadStatus", "statuses": [200]} |

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

- <a id="pa-bce4da57db"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/http_binding/operations/13`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dbd839d0bad5a8fbdc7c0bda5f74e6ac37951fb6b10458e401a7e675c2ea84cf -->

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
  "path": "/v1/reads/status",
  "path_parameters": [],
  "request": {
    "kind": "json",
    "schema": "ReadPreparationRequest"
  },
  "response": {
    "headers": [],
    "kind": "json",
    "schema": "ReadStatus",
    "statuses": [
      200
    ]
  }
}
```
