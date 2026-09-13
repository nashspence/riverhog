# stove0_protocol.BranchSetDecision.leaf_branches

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-branchsetdecision-leaf-branches:1463dc948b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-550caf4d79"></a>
| Field | Shape |
|---|---|
| <a id="s-865dfab2b1"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-7873ecebaf"></a>`distribution` | "stove0-protocol" |
| <a id="s-b48428990a"></a>`module` | "stove0_protocol" |
| <a id="s-b282f8e68a"></a>`name` | "leaf_branches" |
| <a id="s-8742c9d896"></a>`owner` | "stove0_protocol.BranchSetDecision" |
| <a id="s-25d68fe093"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.BranchSetDecision](stove0-protocol-branchsetdecision.md)

## Governing policies

- <a id="pa-8f51649ab1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.BranchSetDecision.leaf_branches`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 41e8b1711160a2a5593a1d71aac3a66921bc8bfc2d1dc25f7e14a56e6d5869ca -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'tuple[BranchPlan, ...]'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "leaf_branches",
  "owner": "stove0_protocol.BranchSetDecision",
  "unit": "member"
}
```
