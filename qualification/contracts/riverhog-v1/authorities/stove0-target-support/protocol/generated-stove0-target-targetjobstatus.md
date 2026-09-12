# generated:stove0-target: TargetJobStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:stove0-target-support:generated-stove0-target-targetjobstatus:b1d1aa5cfc -->

| Audit field | Value |
|---|---|
| Authority | `stove0-target-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 4 |

## Machine authority

- `/external_contract/protocol_schemas/generated:stove0-target/schemas/TargetJobStatus`

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
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract

- `title`: TargetJobStatus
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `attempt` | yes | integer |  |
| `derivation` | no | object (3 fields) |  |
| `effect_receipt` | no | object (2 fields) |  |
| `execution_evidence` | no | object (2 fields) |  |
| `failure` | no | object (2 fields) |  |
| `inapplicable` | no | object (2 fields) |  |
| `job_id` | yes | string |  |
| `output_collection` | no | object (2 fields) |  |
| `plan_sha256` | yes | string |  |
| `production` | no | object (2 fields) |  |
| `progress` | yes | #/$defs/TargetProgress |  |
| `protocol` | no | string |  |
| `request_sha256` | yes | string |  |
| `state` | yes | string |  |

### Definitions

| Definition | Shape |
|---|---|
| `ArtifactDispositionSetIdentity` | object |
| `CollectionId` | integer |
| `ExternalEffectReceipt` | object |
| `JsonValue` | object (0 fields) |
| `OutputArtifactRoleCount` | object |
| `OutputArtifactSetIdentity` | object |
| `OutputCollectionRef` | object |
| `TargetExecutionEvidence` | object |
| `TargetFailure` | object |
| `TargetInapplicable` | object |
| `TargetProductionAuthority` | object |
| `TargetProgress` | object |
