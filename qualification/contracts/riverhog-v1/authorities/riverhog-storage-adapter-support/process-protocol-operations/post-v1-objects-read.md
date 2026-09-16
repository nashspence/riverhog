# POST /v1/objects/read

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-operations:riverhog-storage-adapter-support:post-v1-objects-read:f86105c7c3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Operations](index.md) |

## External contract

<a id="s-828c5ba228"></a>
| Concern | Contract |
|---|---|
| <a id="s-0c05b742ac"></a>`error_schema` | StorageAdapterError |
| <a id="s-907059ef31"></a>`errors` | [{"code": "unauthorized", "status": 401}, {"code": "invalid_request", "status": 400}, {"code": "provider_unavailable", "status": 503}, {"code": "internal_failure", "status": 500}, {"code": "request_too_large", "status": 413}, {"code": "invalid_path", "status": 400}, {"code": "not_found", "status": 404}, {"code": "invalid_range", "status": 416}, {"code": "read_not_ready", "status": 409}, {"code": "read_expired", "status": 409}, {"code": "integrity_failure", "status": 409}] |
| <a id="s-fe835396ef"></a>`method` | POST |
| <a id="s-e0a7ad35a5"></a>`path` | /v1/objects/read |
| <a id="s-07f05906e5"></a>`path_parameters` | [] |
| <a id="s-31b93349ad"></a>`request` | {"kind": "json", "schema": "ObjectReadRequest"} |
| <a id="s-651364b985"></a>`response` | {"headers": [{"description": "Exact framed response-body length in bytes.", "name": "Content-Length", "schema": {"type": "string"}}, {"description": "Adapter-observed complete object length in bytes.", "name": "X-Riverhog-Object-Bytes", "schema": {"type": "string"}}, {"description": "Opaque provider revision when one exists.", "name": "X-Riverhog-Object-Revision", "schema": {"type": "string"}}, {"description": "Exact returned range for a nonempty ranged read.", "name": "Content-Range", "schema": {"type": "string"}}], "kind": "framed", "schema": "ObjectReadReceipt", "statuses": [200, 206]} |

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

- <a id="pa-15cbe7da00"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/http_binding/operations/9`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ff38b1951537bf8291dad5b8cc4ea87d221a0d43d52e5222529cbe6917b98ecf -->

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
      "code": "invalid_range",
      "status": 416
    },
    {
      "code": "read_not_ready",
      "status": 409
    },
    {
      "code": "read_expired",
      "status": 409
    },
    {
      "code": "integrity_failure",
      "status": 409
    }
  ],
  "method": "POST",
  "path": "/v1/objects/read",
  "path_parameters": [],
  "request": {
    "kind": "json",
    "schema": "ObjectReadRequest"
  },
  "response": {
    "headers": [
      {
        "description": "Exact framed response-body length in bytes.",
        "name": "Content-Length",
        "schema": {
          "type": "string"
        }
      },
      {
        "description": "Adapter-observed complete object length in bytes.",
        "name": "X-Riverhog-Object-Bytes",
        "schema": {
          "type": "string"
        }
      },
      {
        "description": "Opaque provider revision when one exists.",
        "name": "X-Riverhog-Object-Revision",
        "schema": {
          "type": "string"
        }
      },
      {
        "description": "Exact returned range for a nonempty ranged read.",
        "name": "Content-Range",
        "schema": {
          "type": "string"
        }
      }
    ],
    "kind": "framed",
    "schema": "ObjectReadReceipt",
    "statuses": [
      200,
      206
    ]
  }
}
```

</details>
