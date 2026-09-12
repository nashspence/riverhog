# generated:riverhog-storage-adapter: ObjectReadRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-objectreadrequest:713e3abc38 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/ObjectReadRequest`

## Effective policies

- `compatibility/components/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:riverhog-storage-adapter` — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`
- Proof: `make dist-smoke`
- Proof: `make build`

## Contract

- `title`: ObjectReadRequest
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `expected_bytes` | yes | integer |  |
| `object` | yes | #/$defs/ObjectLocator |  |
| `offset` | no | object (3 fields) |  |
| `size` | no | object (3 fields) |  |

### Definitions

| Definition | Shape |
|---|---|
| `ObjectLocator` | object |
