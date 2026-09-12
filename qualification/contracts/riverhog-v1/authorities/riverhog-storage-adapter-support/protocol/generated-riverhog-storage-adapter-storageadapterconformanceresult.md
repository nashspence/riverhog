# generated:riverhog-storage-adapter: StorageAdapterConformanceResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-storag-b0b8ffd825:e78895edd0 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/StorageAdapterConformanceResult`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:riverhog-storage-adapter` — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `contract_max` | maximum=120, minimum=1, reason=schema-maximum |

## Contract

- `title`: StorageAdapterConformanceResult
- `description`: Stable positive evidence returned after the complete check set passes.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `checks` | yes | array |  |
| `coverage` | no | string |  |
| `descriptor` | yes | #/$defs/AdapterDescriptor |  |
| `format` | no | string |  |
| `protocol` | no | string |  |
| `status` | no | string |  |

### Definitions

| Definition | Shape |
|---|---|
| `AdapterDescriptor` | object |
