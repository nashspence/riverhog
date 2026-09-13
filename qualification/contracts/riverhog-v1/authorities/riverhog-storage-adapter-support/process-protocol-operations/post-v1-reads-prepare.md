# POST /v1/reads/prepare

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-operations:riverhog-storage-adapter-support:post-v1-reads-prepare:6a06eaccbd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Operations](index.md) |

## External contract

<a id="s-0117660bf1"></a>
| Concern | Contract |
|---|---|
| <a id="s-f60587a842"></a>`error_schema` | StorageAdapterError |
| <a id="s-85db94d80a"></a>`errors` | [{"code": "unauthorized", "status": 401}, {"code": "invalid_request", "status": 400}, {"code": "provider_unavailable", "status": 503}, {"code": "internal_failure", "status": 500}, {"code": "request_too_large", "status": 413}, {"code": "invalid_path", "status": 400}, {"code": "not_found", "status": 404}, {"code": "read_not_ready", "status": 409}, {"code": "read_expired", "status": 409}] |
| <a id="s-782787daca"></a>`method` | POST |
| <a id="s-eaaaeeab53"></a>`path` | /v1/reads/prepare |
| <a id="s-90095faf3d"></a>`path_parameters` | [] |
| <a id="s-391ba8a408"></a>`request` | {"kind": "json", "schema": "ReadPreparationRequest"} |
| <a id="s-4f30faba97"></a>`response` | {"headers": [], "kind": "json", "schema": "ReadStatus", "statuses": [200]} |

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

- <a id="pa-48d1418123"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/http_binding/operations/12`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a0fedcfe0921c2a101a5bfb991423e197ecdf6ab9737d2b31105b77a845d17de -->

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
  "path": "/v1/reads/prepare",
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
