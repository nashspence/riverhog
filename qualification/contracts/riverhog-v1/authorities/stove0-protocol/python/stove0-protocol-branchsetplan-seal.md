# stove0_protocol.BranchSetPlan.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-branchsetplan-seal:7fbf6d00d4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-91002ad5f8"></a>
- <a id="s-a74b24aa53"></a>`distribution`: `stove0-protocol`
- <a id="s-e518ca49fc"></a>`module`: `stove0_protocol`
- <a id="s-c6d933d048"></a>`name`: `seal`
- <a id="s-46949f3681"></a>`owner`: `stove0_protocol.BranchSetPlan`
- <a id="s-a4474bb1dc"></a>`unit`: `member`

### Declared structure

- <a id="s-2f44ec25dd"></a>`kind`: `"classmethod"`
- <a id="s-79bc20af80"></a>`signature`: `"\"(cls, *, parent_work: 'WorkIdentity', decision_sha256: 'str', evidence_sha256s: 'Sequence[str]' = (), branches: 'Sequence[BranchDeclaration]', join: 'JoinDeclaration \| None' = None, retirement_policy: 'RetirementPolicy' = 'retain', retirement_grace_seconds: 'int' = 0, selections: 'SelectionDocuments', branch_sets: 'Mapping[str, BranchSetPlan] \| None' = None) -> 'BranchSetPlan'\""`

## Maintained corroboration

### Related interface records

- [BranchSetPlan](stove0-protocol-branchsetplan.md)

## Governing policies

- <a id="pa-74f4caed6b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.BranchSetPlan.seal`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 240ac3842fe4267c68e6d12f97b6aa31246b490fe2aa2fcae2c00008590bb08c -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, *, parent_work: 'WorkIdentity', decision_sha256: 'str', evidence_sha256s: 'Sequence[str]' = (), branches: 'Sequence[BranchDeclaration]', join: 'JoinDeclaration | None' = None, retirement_policy: 'RetirementPolicy' = 'retain', retirement_grace_seconds: 'int' = 0, selections: 'SelectionDocuments', branch_sets: 'Mapping[str, BranchSetPlan] | None' = None) -> 'BranchSetPlan'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "seal",
  "owner": "stove0_protocol.BranchSetPlan",
  "unit": "member"
}
```
