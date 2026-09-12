# generated:riverhog-storage-adapter: StorageAdapterError

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-storag-7d05cfb36b:316e45aee7 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/StorageAdapterError`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:riverhog-storage-adapter` — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `contract_max` | maximum=2000, minimum=1, reason=schema-maximum |

## Contract summary

- `title`: StorageAdapterError
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `error` | yes | #/$defs/StorageAdapterErrorBody |  |

### Definitions

| Definition | Shape |
|---|---|
| `StorageAdapterErrorBody` | object |

## Complete owned contract

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
