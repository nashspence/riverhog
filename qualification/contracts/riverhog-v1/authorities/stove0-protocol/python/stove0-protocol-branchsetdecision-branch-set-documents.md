# stove0_protocol.BranchSetDecision.branch_set_documents

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-branchsetdecision-branch-b8c559fb6e:c1e56100f1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9634ab13e4"></a>
- <a id="s-bc491b16d2"></a>`distribution`: `stove0-protocol`
- <a id="s-1945203277"></a>`module`: `stove0_protocol`
- <a id="s-f8d706512d"></a>`name`: `branch_set_documents`
- <a id="s-99b28a3882"></a>`owner`: `stove0_protocol.BranchSetDecision`
- <a id="s-83f16392f2"></a>`unit`: `member`

### Declared structure

- <a id="s-a51c5f33ed"></a>`kind`: `"property"`
- <a id="s-de9398bb2d"></a>`signature`: `"\"(self) -> 'dict[str, BranchSetPlan]'\""`

## Maintained corroboration

### Related interface records

- [stove0_protocol.BranchSetDecision](stove0-protocol-branchsetdecision.md)

## Governing policies

- <a id="pa-318d232665"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.BranchSetDecision.branch_set_documents`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d3a5f02a095b252ab077ddeda45958965220f2af375ccad47d1d0ec7c8fb5c73 -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'dict[str, BranchSetPlan]'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "branch_set_documents",
  "owner": "stove0_protocol.BranchSetDecision",
  "unit": "member"
}
```
