# stove0_protocol.BranchPlan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-branchplan:03d66abe2c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7eaf409fc2"></a>
| Field | Shape |
|---|---|
| <a id="s-6f42efa64a"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-1c53b5d912"></a>`distribution` | "stove0-protocol" |
| <a id="s-c8cb5e6e65"></a>`module` | "stove0_protocol" |
| <a id="s-c5641b2544"></a>`name` | "BranchPlan" |
| <a id="s-d5c26121bf"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.BranchPlan.bind_child_work](stove0-protocol-branchplan-bind-child-work.md)
- [stove0_protocol.BranchPlan.build](stove0-protocol-branchplan-build.md)

## Governing policies

- <a id="pa-639ccb615e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.BranchPlan`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 82f958f1aa037efe64d003d547ecdd016a69e16c87b42688e59250835657ed35 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "d046c81f10004092168d1b8ac2d4f31967cb97231750a656d0c8cbf085468178",
    "signature": "\"(*, kind: Literal['leaf'] = 'leaf', branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], artifact_selection: stove0_protocol.fork_join.ArtifactSelectionRef, workflow_plan: stove0_protocol.models.WorkflowPlan) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "BranchPlan",
  "unit": "export"
}
```
