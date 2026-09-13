# stove0_protocol.BranchSetEvaluation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-branchsetevaluation:9176523a64 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-17d7bf5ef7"></a>
| Field | Shape |
|---|---|
| <a id="s-d096ef6568"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-2a9345ab29"></a>`distribution` | "stove0-protocol" |
| <a id="s-027d20e8f4"></a>`module` | "stove0_protocol" |
| <a id="s-5abec8d0f8"></a>`name` | "BranchSetEvaluation" |
| <a id="s-cb703fb53f"></a>`unit` | "export" |

## Governing policies

- <a id="pa-893a4051d5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.BranchSetEvaluation`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d9cb87724d9acdb4bce2cae101d14b2432edb383da189e09cb106f98f3551875 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "cf1eb8915ebea499a8253c4e4fca1c5582de70e008456ec3d105232989796362",
    "signature": "\"(*, branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], succeeded_branches: tuple[stove0_protocol.fork_join.BranchSettlement, ...], succeeded_effects: tuple[stove0_protocol.fork_join.BranchEffectSettlement, ...], succeeded_coordinations: tuple[stove0_protocol.fork_join.CoordinationSettlement, ...], unsettled_branch_ids: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], failed_branch_ids: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], inapplicable_branch_ids: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], interrupted_branch_ids: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], canceled_branch_ids: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], join_ready: bool, resolved_join_plan: stove0_protocol.fork_join.JoinPlan | None, join_state: Literal['not-declared', 'waiting', 'ready', 'succeeded', 'failed', 'inapplicable', 'interrupted', 'canceled'], join_settlement: stove0_protocol.fork_join.JoinSettlement | None, unsettled_work_ids: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], ...], branch_set_succeeded: bool, coordination_settlement: stove0_protocol.fork_join.CoordinationSettlement | None, retirement_requested: bool, coordination_complete_for_retirement: bool) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "BranchSetEvaluation",
  "unit": "export"
}
```
