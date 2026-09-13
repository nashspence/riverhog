# stove0_protocol.CoordinationBranchPlan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-coordinationbranchplan:f23e6c90ab -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fc1bebe5d6"></a>
| Field | Shape |
|---|---|
| <a id="s-c39c364afd"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-9b962a3eb3"></a>`distribution` | "stove0-protocol" |
| <a id="s-2fd891f6e0"></a>`module` | "stove0_protocol" |
| <a id="s-6a5a490ffa"></a>`name` | "CoordinationBranchPlan" |
| <a id="s-d4f539a06f"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.CoordinationBranchPlan.bind_child_work](stove0-protocol-coordinationbranchplan-bind-child-work.md)
- [stove0_protocol.CoordinationBranchPlan.build_work](stove0-protocol-coordinationbranchplan-build-work.md)
- [stove0_protocol.CoordinationBranchPlan.build](stove0-protocol-coordinationbranchplan-build.md)

## Governing policies

- <a id="pa-571e354fc0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.CoordinationBranchPlan`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f80a1735c7335994cfc8546b664d174714cf75dbf1498b2705d0c06a69a42053 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "fbeeb2288a21de92836dad26dc3ecea68cdd3d8cb85863660d91fc66420ea16d",
    "signature": "\"(*, kind: Literal['coordination'] = 'coordination', branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], artifact_selection: stove0_protocol.fork_join.ArtifactSelectionRef, work: stove0_protocol.models.WorkIdentity, branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "CoordinationBranchPlan",
  "unit": "export"
}
```
