# stove0_protocol.CoordinationBranchPlan.build

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-coordinationbranchplan-build:f3e0235aea -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-181ac68f4e"></a>
- <a id="s-c8a48c25e7"></a>`distribution`: `stove0-protocol`
- <a id="s-4df7a773a4"></a>`module`: `stove0_protocol`
- <a id="s-8901bb2313"></a>`name`: `build`
- <a id="s-7729befbf8"></a>`owner`: `stove0_protocol.CoordinationBranchPlan`
- <a id="s-36424a43c7"></a>`unit`: `member`

### Declared structure

- <a id="s-e419aa05a2"></a>`kind`: `"classmethod"`
- <a id="s-f79077973a"></a>`signature`: `"\"(cls, *, parent_work: 'WorkIdentity', branch_id: 'str', decision_sha256: 'str', selection: 'ArtifactSelection', recipe: 'RecipeRef', effective_intent: 'Mapping[str, JsonValue]', branch_set_sha256: 'str') -> 'CoordinationBranchPlan'\""`

## Maintained corroboration

### Related interface records

- [CoordinationBranchPlan](stove0-protocol-coordinationbranchplan.md)

## Governing policies

- <a id="pa-7841dc0eff"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.CoordinationBranchPlan.build`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 150857655338bf843d2a33a02d8cf897dafa9152b263a60ea2004358994030ef -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, *, parent_work: 'WorkIdentity', branch_id: 'str', decision_sha256: 'str', selection: 'ArtifactSelection', recipe: 'RecipeRef', effective_intent: 'Mapping[str, JsonValue]', branch_set_sha256: 'str') -> 'CoordinationBranchPlan'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "build",
  "owner": "stove0_protocol.CoordinationBranchPlan",
  "unit": "member"
}
```

</details>
