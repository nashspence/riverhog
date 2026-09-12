# generated:riverhog-storage-adapter: StorageAdapterError

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-storag-7d05cfb36b:316e45aee7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

- `title`: StorageAdapterError
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `error` | yes | #/$defs/StorageAdapterErrorBody |  |

### Definitions

| Definition | Shape |
|---|---|
| `StorageAdapterErrorBody` | type="object"; fields=`code`, `message`; additional keys=`additionalProperties`, `required` |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| length | characters | `contract_max` | maximum=2000, minimum=1, reason=schema-maximum |

## Governing policies

- `compatibility/components/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make dist-smoke`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:riverhog-storage-adapter` — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

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
