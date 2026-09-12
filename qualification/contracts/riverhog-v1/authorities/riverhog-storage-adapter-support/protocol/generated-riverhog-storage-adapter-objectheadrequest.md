# generated:riverhog-storage-adapter: ObjectHeadRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-objectheadrequest:524e2da9c9 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/ObjectHeadRequest`

## Effective policies

- `compatibility/components/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:riverhog-storage-adapter` — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`
- Proof: `make dist-smoke`
- Proof: `make build`

## Contract

- `title`: ObjectHeadRequest
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `expected_placement` | yes | string |  |
| `object` | yes | #/$defs/ObjectLocator |  |

### Definitions

| Definition | Shape |
|---|---|
| `ObjectLocator` | object |
