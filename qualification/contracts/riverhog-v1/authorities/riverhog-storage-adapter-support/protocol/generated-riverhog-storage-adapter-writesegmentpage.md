# generated:riverhog-storage-adapter: WriteSegmentPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-writesegmentpage:f462fcb7a3 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 4 |

## Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/WriteSegmentPage`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/bounded-segment/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:riverhog-storage-adapter` — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | maximum=128, minimum=None, reason=bounded-storage-write-segment-page |
| length | characters | `contract_max` | maximum=4000, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=4000, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract

- `title`: WriteSegmentPage
- `description`: One bounded page under an adapter-owned immutable traversal view.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `completion` | no | object (2 fields) |  |
| `next_after_number` | no | object (3 fields) |  |
| `segments` | no | array |  |
| `session` | yes | #/$defs/WriteSession |  |
| `traversal_token` | yes | string |  |

### Definitions

| Definition | Shape |
|---|---|
| `WriteCompletionAuthority` | object |
| `WriteSegmentReceipt` | object |
| `WriteSession` | object |
