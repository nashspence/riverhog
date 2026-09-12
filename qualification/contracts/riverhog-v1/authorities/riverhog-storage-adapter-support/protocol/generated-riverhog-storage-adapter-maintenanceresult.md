# generated:riverhog-storage-adapter: MaintenanceResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-maintenanceresult:0f44d30e3c -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/MaintenanceResult`

## Effective policies

- `compatibility/components/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:riverhog-storage-adapter` — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`
- Proof: `make dist-smoke`
- Proof: `make build`

## Contract summary

- `title`: MaintenanceResult
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `affected` | yes | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1b8ad376bb41a2eb81c64450d4ccb99a09150f8f1c72f6203ecdad2b933a565c -->

```json
{
  "additionalProperties": false,
  "properties": {
    "affected": {
      "minimum": 0,
      "title": "Affected",
      "type": "integer"
    }
  },
  "required": [
    "affected"
  ],
  "title": "MaintenanceResult",
  "type": "object"
}
```
