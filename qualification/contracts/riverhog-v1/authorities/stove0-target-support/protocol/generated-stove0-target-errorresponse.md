# generated:stove0-target: ErrorResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:stove0-target-support:generated-stove0-target-errorresponse:0ce148e260 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-target-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/protocol_schemas/generated:stove0-target/schemas/ErrorResponse`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/no-semantic-maximum/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:stove0-target` — `reference/stove0/packages/target-support/src/stove0_target_support/schemas.py::target_schema_bundle`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract

- `title`: ErrorResponse
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `error` | yes | #/$defs/ErrorBody |  |

### Definitions

| Definition | Shape |
|---|---|
| `ErrorBody` | object |
