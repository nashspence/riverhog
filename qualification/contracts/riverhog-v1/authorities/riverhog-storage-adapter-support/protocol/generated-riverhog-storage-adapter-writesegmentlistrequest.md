# generated:riverhog-storage-adapter: WriteSegmentListRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-writes-3419a4c482:3017df454c -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/WriteSegmentListRequest`

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
| value | schema-value | `contract_max` | maximum=128, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=4000, minimum=1, reason=schema-maximum |

## Contract

- `title`: WriteSegmentListRequest
- `description`: Request one bounded page from an exact accepted-segment view.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `after_number` | no | integer |  |
| `maximum_items` | no | integer |  |
| `session` | yes | #/$defs/WriteSession |  |
| `traversal_token` | no | object (3 fields) |  |

### Definitions

| Definition | Shape |
|---|---|
| `WriteSession` | object |
