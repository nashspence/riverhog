# stove0_protocol.WorkflowPreview

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowpreview:bbf475d7e1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-651702325b"></a>
| Field | Shape |
|---|---|
| <a id="s-7227987cee"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-812cee1ddd"></a>`distribution` | "stove0-protocol" |
| <a id="s-a0917f42b4"></a>`module` | "stove0_protocol" |
| <a id="s-e79104d0ec"></a>`name` | "WorkflowPreview" |
| <a id="s-d96e0e3c3f"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.WorkflowPreview.seal](stove0-protocol-workflowpreview-seal.md)
- [stove0_protocol.WorkflowPreview.verify_digest](stove0-protocol-workflowpreview-verify-digest.md)

## Governing policies

- <a id="pa-006a1cbc2a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPreview`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 83c9dca49f53fb2faa0e17e19bb0a4b6211d2646360e9a592df0d19022d9dd68 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "6151382a075394584ab8c2e21fde7b1cf1955791b21e983ab5119792ed3f02de",
    "signature": "\"(*, format: Literal['stove0-workflow-preview/v1'] = 'stove0-workflow-preview/v1', preview_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], state: Literal['ready', 'inapplicable', 'failed', 'canceled'], work: stove0_protocol.models.WorkIdentity, observations: tuple[stove0_protocol.models.ObservationEvidence, ...] = (), branch_set_plan: stove0_protocol.fork_join.BranchSetPlan | None = None, branch_sets: tuple[stove0_protocol.fork_join.BranchSetPlan, ...] = (), selections: tuple[stove0_protocol.fork_join.ArtifactSelection, ...] = (), target_plans: tuple[stove0_protocol.fork_join.BranchTargetPreview, ...] = (), outcome: stove0_protocol.models.PreviewOutcome | None = None, warnings: tuple[str, ...] = (), preview_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "WorkflowPreview",
  "unit": "export"
}
```
