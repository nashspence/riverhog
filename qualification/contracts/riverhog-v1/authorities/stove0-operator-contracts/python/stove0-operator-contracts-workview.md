# stove0_operator_contracts.WorkView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workview:fa1e81cc66 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5543e41a2d"></a>
| Field | Shape |
|---|---|
| <a id="s-7cd9480da7"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-0f9e34c87d"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-0d3f0d932c"></a>`module` | "stove0_operator_contracts" |
| <a id="s-10c7e78d48"></a>`name` | "WorkView" |
| <a id="s-c30d3c568c"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_operator_contracts.WorkView.exact_identity](stove0-operator-contracts-workview-exact-identity.md)
- [stove0_operator_contracts.WorkView.from_record](stove0-operator-contracts-workview-from-record.md)

## Governing policies

- <a id="pa-198c4952db"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkView`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 60bc6c61fd648872c4c394dc7f0034ccd19c566f0b39e6df7e28859642c8f14f -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "4653eff1d6b27ef7d65f8577e7299e62b66330454054717075c27843f3dc1246",
    "signature": "\"(*, format: Literal['stove0-work-view/v1'] = 'stove0-work-view/v1', work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], work: stove0_protocol.models.WorkIdentity, phase: Literal['eligible', 'claimed', 'observing', 'planning', 'target_preflight', 'queued', 'executing', 'output_finalizing', 'verifying', 'settled', 'retirement_pending', 'coordinating', 'abandon_pending', 'complete', 'inapplicable', 'failed', 'canceled'], revision: Annotated[int, Ge(ge=1)], claim: stove0_operator_contracts.WorkClaimView | None = None, preview_acceptance: stove0_operator_contracts.PreviewAcceptanceView | None = None, expected_target_plan_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, observation_requests: tuple[stove0_protocol.models.ObservationRequest, ...] = (), observation_results: tuple[stove0_protocol.models.ObservationResult, ...] = (), branch_set_plan: stove0_protocol.fork_join.BranchSetPlan | None = None, coordination_settlement: stove0_protocol.fork_join.CoordinationSettlement | None = None, join_plan: stove0_protocol.fork_join.JoinPlan | None = None, coordination_cancel_requested: bool = False, workflow_plan: stove0_protocol.models.WorkflowPlan | None = None, target_plan: Optional[Annotated[stove0_target_protocol.protocol.TransformPlan | stove0_target_protocol.protocol.EffectPlan, FieldInfo(annotation=NoneType, required=True, discriminator='protocol')]] = None, controller_evidence: stove0_protocol.models.ControllerEvidence | None = None, target_request: stove0_target_protocol.protocol.AcceptedTargetJob | None = None, target_status: stove0_target_protocol.protocol.TargetJobStatus | None = None, output: stove0_target_protocol.protocol.OutputCollectionRef | None = None, target_settlement: stove0_target_protocol.protocol.TargetSettlementAuthority | None = None, retirement_remaining: tuple[int, ...] = (), failure: stove0_operator_contracts.WorkFailureView | None = None, inapplicable: stove0_operator_contracts.WorkInapplicableView | None = None, abandon_outcome: Optional[Literal['inapplicable', 'failed', 'canceled']] = None) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "WorkView",
  "unit": "export"
}
```
