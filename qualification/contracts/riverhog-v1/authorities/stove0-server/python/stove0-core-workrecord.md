# stove0_core.WorkRecord

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workrecord:6c77e989e0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bf838cb901"></a>
| Field | Shape |
|---|---|
| <a id="s-7d1d118649"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-5904376c13"></a>`distribution` | "stove0-server" |
| <a id="s-fce8b221a1"></a>`module` | "stove0_core" |
| <a id="s-2910eb6032"></a>`name` | "WorkRecord" |
| <a id="s-e048a799a3"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_core.WorkRecord.validate_shape](stove0-core-workrecord-validate-shape.md)
- [stove0_core.WorkRecord.work_id](stove0-core-workrecord-work-id.md)

## Governing policies

- <a id="pa-92a9aa10e7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.WorkRecord`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b24268c6743443be69966334c69bcb20536b41dad70d89d423e8d4454b8c34f5 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "51fdec684c3f09dbe0e9b0c9a25f47a26c9c87165a72fa7d8b768d3a713d2dc5",
    "signature": "\"(*, format: Literal['stove0-work-record/v1'] = 'stove0-work-record/v1', work: stove0_protocol.models.WorkIdentity, phase: Literal['eligible', 'claimed', 'observing', 'planning', 'target_preflight', 'queued', 'executing', 'output_finalizing', 'verifying', 'settled', 'retirement_pending', 'coordinating', 'abandon_pending', 'complete', 'inapplicable', 'failed', 'canceled'] = 'eligible', revision: Annotated[int, Ge(ge=1)] = 1, claim: stove0_core.work_state.ClaimBinding | None = None, preview_acceptance: stove0_core.work_state.PreviewAcceptance | None = None, expected_target_plan_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, observation_requests: tuple[stove0_protocol.models.ObservationRequest, ...] = (), observation_results: tuple[stove0_protocol.models.ObservationResult, ...] = (), branch_set_plan: stove0_protocol.fork_join.BranchSetPlan | None = None, coordination_settlement: stove0_protocol.fork_join.CoordinationSettlement | None = None, join_plan: stove0_protocol.fork_join.JoinPlan | None = None, coordination_cancel_requested: bool = False, workflow_plan: stove0_protocol.models.WorkflowPlan | None = None, target_plan: Optional[Annotated[stove0_target_protocol.protocol.TransformPlan | stove0_target_protocol.protocol.EffectPlan, FieldInfo(annotation=NoneType, required=True, discriminator='protocol')]] = None, controller_evidence: stove0_protocol.models.ControllerEvidence | None = None, target_request: stove0_target_protocol.protocol.AcceptedTargetJob | None = None, target_status: stove0_target_protocol.protocol.TargetJobStatus | None = None, output: stove0_target_protocol.protocol.OutputCollectionRef | None = None, target_settlement: stove0_target_protocol.protocol.TargetSettlementAuthority | None = None, retirement_remaining: tuple[int, ...] = (), failure: stove0_core.work_state.WorkFailure | None = None, inapplicable: stove0_core.work_state.WorkInapplicable | None = None, abandon_outcome: Optional[Literal['inapplicable', 'failed', 'canceled']] = None) -> None\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "WorkRecord",
  "unit": "export"
}
```
