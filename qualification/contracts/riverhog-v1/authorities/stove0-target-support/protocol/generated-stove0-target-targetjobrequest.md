# generated:stove0-target: TargetJobRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:stove0-target-support:generated-stove0-target-targetjobrequest:6f2b53243c -->

| Audit field | Value |
|---|---|
| Authority | `stove0-target-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 5 |

## Machine authority

- `/external_contract/protocol_schemas/generated:stove0-target/schemas/TargetJobRequest`

## Effective policies

- `compatibility/components/v1`
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
| length | characters | `contract_max` | maximum=2048, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=4096, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=4096, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=2048, minimum=1, reason=schema-maximum |

## Contract

- `title`: TargetJobRequest
- `description`: Secret-bearing target invocation; never store this document durably.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `callback_access` | yes | #/$defs/TargetCallbackAccess |  |
| `declaration` | yes | #/$defs/TargetJobDeclaration |  |
| `request_sha256` | yes | string |  |
| `runtime` | yes | #/$defs/TargetRuntimeAuthority |  |

### Definitions

| Definition | Shape |
|---|---|
| `ArtifactSelectionRef` | object |
| `ArtifactSubject` | object |
| `BranchWorkBinding` | object |
| `CollectionId` | integer |
| `CollectionRootRef` | object |
| `ControllerEvidence` | object |
| `EffectPlan` | object |
| `EvaluationBinding` | object |
| `ExecutionEnvelope` | object |
| `JoinWorkBinding` | object |
| `JoinWorkMemberBinding` | object |
| `JsonSchemaDocument` | object |
| `JsonValue` | object (0 fields) |
| `ObservationEvidence` | object |
| `ObservationFailure` | object |
| `ObservationInapplicable` | object |
| `ObservationRequest` | object |
| `ObservationResult` | object |
| `ObserverImplementation` | object |
| `OperationRef` | object |
| `RecipeRef` | object |
| `TargetCallbackAccess` | object |
| `TargetInputAuthority` | object |
| `TargetInputRoleCount` | object |
| `TargetJobDeclaration` | object |
| `TargetPlanBinding` | object |
| `TargetRuntimeAuthority` | object |
| `TransformPlan` | object |
| `WorkIdentity` | object |
| `WorkflowPlan` | object |
