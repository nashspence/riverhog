# generated:riverhog-storage-adapter: StorageAdapterError

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-storag-7d05cfb36b:316e45aee7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-81e0279481"></a>
- <a id="s-15a4c3c3d3"></a>`title`: StorageAdapterError
- <a id="s-d54a2fa63d"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-896bc94219"></a>`error` | yes | #/$defs/StorageAdapterErrorBody |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-16d9d4653c"></a>`StorageAdapterErrorBody` | type="object"; fields=`code`, `message`; additional keys=`additionalProperties`, `required` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=2000; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-396a1bf2d8"></a>[definition StorageAdapterErrorBody · field message](#s-16d9d4653c) | `length · characters · contract_max` | shared above |

## Governing policies

- <a id="pa-8508a40539"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-455964c65f"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/StorageAdapterError`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 19b906afcdf8d94d77f5aef65b4facf0cb084d382ef94d440ae8a5cd58e7aa7f -->

```json
{
  "$defs": {
    "StorageAdapterErrorBody": {
      "additionalProperties": false,
      "properties": {
        "code": {
          "enum": [
            "unauthorized",
            "invalid_request",
            "not_found",
            "method_not_allowed",
            "length_required",
            "request_too_large",
            "insufficient_storage",
            "identity_conflict",
            "traversal_invalidated",
            "invalid_path",
            "invalid_range",
            "read_not_ready",
            "read_expired",
            "integrity_failure",
            "provider_unavailable",
            "internal_failure"
          ],
          "title": "Code",
          "type": "string"
        },
        "message": {
          "maxLength": 2000,
          "minLength": 1,
          "title": "Message",
          "type": "string"
        }
      },
      "required": [
        "code",
        "message"
      ],
      "title": "StorageAdapterErrorBody",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
    "error": {
      "$ref": "#/$defs/StorageAdapterErrorBody"
    }
  },
  "required": [
    "error"
  ],
  "title": "StorageAdapterError",
  "type": "object"
}
```
