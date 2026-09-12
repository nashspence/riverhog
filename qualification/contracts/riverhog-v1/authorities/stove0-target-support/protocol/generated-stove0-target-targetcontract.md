# generated:stove0-target: TargetContract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:stove0-target-support:generated-stove0-target-targetcontract:5398dc2d99 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-target-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 5 |

## Machine authority

- `/external_contract/protocol_schemas/generated:stove0-target/schemas/TargetContract`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:stove0-target` — `reference/stove0/packages/target-support/src/stove0_target_support/schemas.py::target_schema_bundle`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `contract_max` | maximum=120, minimum=1, reason=schema-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `contract_max` | maximum=200, minimum=1, reason=schema-maximum |

## Contract

- `title`: TargetContract
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `contract_sha256` | yes | string |  |
| `image_digest` | yes | string |  |
| `implementation_id` | yes | string |  |
| `implementation_version` | yes | string |  |
| `operations` | yes | array |  |
| `protocol` | no | string |  |
| `source_revision` | yes | string |  |
| `transport` | no | string |  |

### Definitions

| Definition | Shape |
|---|---|
| `JsonSchemaDocument` | object |
| `JsonValue` | object (0 fields) |
| `TargetOperationSupport` | object |
