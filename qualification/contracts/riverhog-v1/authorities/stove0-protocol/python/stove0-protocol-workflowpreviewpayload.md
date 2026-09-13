# stove0_protocol.WorkflowPreviewPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowpreviewpayload:84aca37c7e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2461dd6b8e"></a>
| Field | Shape |
|---|---|
| <a id="s-bf7b95a88a"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-5ea870c95d"></a>`distribution` | "stove0-protocol" |
| <a id="s-d6d5456dcd"></a>`module` | "stove0_protocol" |
| <a id="s-ff0d754d7e"></a>`name` | "WorkflowPreviewPayload" |
| <a id="s-38713e24cf"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.WorkflowPreviewPayload.canonical_warnings](stove0-protocol-workflowpreviewpayload-canonical-warnings.md)
- [stove0_protocol.WorkflowPreviewPayload.canonical_target_plans](stove0-protocol-workflowpreviewpayload-canonical-target-plans.md)
- [stove0_protocol.WorkflowPreviewPayload.canonical_observations](stove0-protocol-workflowpreviewpayload-canonical-observations.md)
- [stove0_protocol.WorkflowPreviewPayload.canonical_selections](stove0-protocol-workflowpreviewpayload-canonical-selections.md)
- [stove0_protocol.WorkflowPreviewPayload.canonical_child_branch_sets](stove0-protocol-workflowpreviewpayload-canonical-child-branch-sets.md)
- [stove0_protocol.WorkflowPreviewPayload.validate_state](stove0-protocol-workflowpreviewpayload-validate-state.md)

## Governing policies

- <a id="pa-3d08a5cfc9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPreviewPayload`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2946d1132ad55f729c1d81abb40788830a35df909c8ace4642b3adf2b5a02348 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "d00182a18c04ab8c9d3c43ca2ae7af712130f80e1cb52991252abfadbddcb4a9",
    "signature": "\"(*, format: Literal['stove0-workflow-preview/v1'] = 'stove0-workflow-preview/v1', preview_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], state: Literal['ready', 'inapplicable', 'failed', 'canceled'], work: stove0_protocol.models.WorkIdentity, observations: tuple[stove0_protocol.models.ObservationEvidence, ...] = (), branch_set_plan: stove0_protocol.fork_join.BranchSetPlan | None = None, branch_sets: tuple[stove0_protocol.fork_join.BranchSetPlan, ...] = (), selections: tuple[stove0_protocol.fork_join.ArtifactSelection, ...] = (), target_plans: tuple[stove0_protocol.fork_join.BranchTargetPreview, ...] = (), outcome: stove0_protocol.models.PreviewOutcome | None = None, warnings: tuple[str, ...] = ()) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "WorkflowPreviewPayload",
  "unit": "export"
}
```
