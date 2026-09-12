# generated:riverhog-storage-adapter: DeletePrefixRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-delete-ac2392c700:24fdf1c683 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/DeletePrefixRequest`

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
| length | characters | `contract_max` | maximum=4096, minimum=1, reason=schema-maximum |

## Contract summary

- `title`: DeletePrefixRequest
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `mode` | no | string |  |
| `object_prefix` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 26f1798a7e22e1a72e0c6932bfbe66dca3e80273a417c3e5c18fdc0e4bd59a3f -->

```json
{
  "additionalProperties": false,
  "properties": {
    "mode": {
      "const": "all_versions",
      "default": "all_versions",
      "title": "Mode",
      "type": "string"
    },
    "object_prefix": {
      "maxLength": 4096,
      "minLength": 1,
      "title": "Object Prefix",
      "type": "string"
    }
  },
  "required": [
    "object_prefix"
  ],
  "title": "DeletePrefixRequest",
  "type": "object"
}
```
