# generated:riverhog-storage-adapter: AdapterDescriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-adapterdescriptor:fd5ddc3b83 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/AdapterDescriptor`

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
| length | characters | `contract_max` | maximum=120, minimum=1, reason=schema-maximum |

## Contract

- `title`: AdapterDescriptor
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `implementation_id` | yes | string |  |
| `implementation_version` | yes | string |  |
| `maximum_segment_bytes` | no | object (3 fields) |  |
| `maximum_segment_count` | no | object (3 fields) |  |
| `minimum_nonfinal_segment_bytes` | yes | integer |  |
| `protocol` | no | string |  |
| `read_mode` | yes | string |  |
