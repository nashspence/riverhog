# stove0_protocol.resolve_join_plan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-resolve-join-plan:b829436771 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-92a7010e2a"></a>
- <a id="s-c92359449c"></a>`distribution`: `stove0-protocol`
- <a id="s-3694538348"></a>`module`: `stove0_protocol`
- <a id="s-5059fa3590"></a>`name`: `resolve_join_plan`
- <a id="s-175f51655b"></a>`unit`: `export`

### Declared structure

- <a id="s-da4c8c8de9"></a>`kind`: `"function"`
- <a id="s-0aa9d8bc7d"></a>`signature`: `"\"(plan: 'BranchSetPlan', selections: 'SelectionDocuments', settlements: 'Sequence[BranchSettlement]', effect_settlements: 'Sequence[BranchEffectSettlement]' = (), coordination_settlements: 'Sequence[CoordinationSettlement]' = (), branch_sets: 'Mapping[str, BranchSetPlan] \| None' = None) -> 'tuple[JoinPlan, tuple[ArtifactSelection, ...]] \| None'\""`

## Governing policies

- <a id="pa-4aa8ef2e64"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.resolve_join_plan`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c56c8d9db31300fe875c97ea8fee145ef61fa1ba2f04b393cc321576faca2088 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(plan: 'BranchSetPlan', selections: 'SelectionDocuments', settlements: 'Sequence[BranchSettlement]', effect_settlements: 'Sequence[BranchEffectSettlement]' = (), coordination_settlements: 'Sequence[CoordinationSettlement]' = (), branch_sets: 'Mapping[str, BranchSetPlan] | None' = None) -> 'tuple[JoinPlan, tuple[ArtifactSelection, ...]] | None'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "resolve_join_plan",
  "unit": "export"
}
```

</details>
