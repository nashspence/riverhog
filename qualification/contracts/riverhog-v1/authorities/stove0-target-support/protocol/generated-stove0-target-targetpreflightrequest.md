# generated:stove0-target: TargetPreflightRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:stove0-target-support:generated-stove0-target-targetpreflightrequest:bd6f90edee -->

| Audit field | Value |
|---|---|
| Authority | `stove0-target-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 4 |

## Machine authority

- `/external_contract/protocol_schemas/generated:stove0-target/schemas/TargetPreflightRequest`

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
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract

- `title`: TargetPreflightRequest
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `inputs` | yes | #/$defs/TargetInputAuthority |  |
| `intent` | yes | object |  |
| `observations` | no | array |  |
| `operation_contract_sha256` | yes | string |  |
| `operation_id` | yes | string |  |
| `protocol` | no | string |  |
| `target_options` | no | object |  |

### Definitions

| Definition | Shape |
|---|---|
| `ArtifactSelectionRef` | object |
| `ArtifactSubject` | object |
| `CollectionId` | integer |
| `CollectionRootRef` | object |
| `JsonSchemaDocument` | object |
| `JsonValue` | object (0 fields) |
| `ObservationEvidence` | object |
| `ObservationFailure` | object |
| `ObservationInapplicable` | object |
| `ObservationRequest` | object |
| `ObservationResult` | object |
| `ObserverImplementation` | object |
| `TargetInputAuthority` | object |
| `TargetInputRoleCount` | object |
