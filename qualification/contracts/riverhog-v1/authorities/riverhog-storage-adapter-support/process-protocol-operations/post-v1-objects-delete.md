# POST /v1/objects/delete

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-operations:riverhog-storage-adapter-support:post-v1-objects-delete:61221ec52b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Operations](index.md) |

## External contract

<a id="s-73a3f144f8"></a>
| Concern | Contract |
|---|---|
| <a id="s-585554c620"></a>`error_schema` | StorageAdapterError |
| <a id="s-06f838e220"></a>`errors` | [{"code": "unauthorized", "status": 401}, {"code": "invalid_request", "status": 400}, {"code": "provider_unavailable", "status": 503}, {"code": "internal_failure", "status": 500}, {"code": "request_too_large", "status": 413}, {"code": "invalid_path", "status": 400}, {"code": "not_found", "status": 404}] |
| <a id="s-f06162e60b"></a>`method` | POST |
| <a id="s-49cd38a4e8"></a>`path` | /v1/objects/delete |
| <a id="s-5b30b378b9"></a>`path_parameters` | [] |
| <a id="s-e466acd8d8"></a>`request` | {"kind": "json", "schema": "DeleteObjectRequest"} |
| <a id="s-87b2fadd2e"></a>`response` | {"headers": [], "kind": "none", "schema": null, "statuses": [204]} |

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

- <a id="pa-0deca7866e"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/http_binding/operations/10`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b9460a4d008869b7f0a2a772a6fef848398164d237fe1dc39b383e8bb1feee17 -->

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
  "path": "/v1/objects/delete",
  "path_parameters": [],
  "request": {
    "kind": "json",
    "schema": "DeleteObjectRequest"
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
