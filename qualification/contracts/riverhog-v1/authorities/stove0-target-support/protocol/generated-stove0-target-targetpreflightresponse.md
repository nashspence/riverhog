# generated:stove0-target: TargetPreflightResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:stove0-target-support:generated-stove0-target-targetpreflightresponse:9ac3ee61c5 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-target-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/protocol_schemas/generated:stove0-target/schemas/TargetPreflightResponse`

## Effective policies

- `compatibility/components/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:stove0-target` — `reference/stove0/packages/target-support/src/stove0_target_support/schemas.py::target_schema_bundle`
- Proof: `make dist-smoke`
- Proof: `make build`

## Contract

- `title`: TargetPreflightResponse
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `plan` | yes | object (3 fields) |  |
| `target` | yes | #/$defs/TargetContract |  |

### Definitions

| Definition | Shape |
|---|---|
| `ArtifactSelectionRef` | object |
| `EffectPlan` | object |
| `JsonSchemaDocument` | object |
| `JsonValue` | object (0 fields) |
| `TargetContract` | object |
| `TargetInputAuthority` | object |
| `TargetInputRoleCount` | object |
| `TargetOperationSupport` | object |
| `TransformPlan` | object |
