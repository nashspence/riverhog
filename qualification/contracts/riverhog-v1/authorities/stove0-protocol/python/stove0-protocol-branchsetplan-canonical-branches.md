# stove0_protocol.BranchSetPlan.canonical_branches

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-branchsetplan-canonical-branches:7a080808c5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1decda76a8"></a>
- <a id="s-9dee43725c"></a>`distribution`: `stove0-protocol`
- <a id="s-7b842941b4"></a>`module`: `stove0_protocol`
- <a id="s-05476b71d3"></a>`name`: `canonical_branches`
- <a id="s-31c7ae0fb9"></a>`owner`: `stove0_protocol.BranchSetPlan`
- <a id="s-7c280807df"></a>`unit`: `member`

### Declared structure

- <a id="s-d8836f8341"></a>`kind`: `"classmethod"`
- <a id="s-69a1ff9d18"></a>`signature`: `"\"(cls, value: 'tuple[BranchDeclaration, ...]') -> 'tuple[BranchDeclaration, ...]'\""`

## Maintained corroboration

### Related interface records

- [BranchSetPlan](stove0-protocol-branchsetplan.md)

## Governing policies

- <a id="pa-bc120a57c7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.BranchSetPlan.canonical_branches`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f2764c4f1938b7f3182a06284a8410895ea12bf7bdc1338cb4e42c714804c042 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[BranchDeclaration, ...]') -> 'tuple[BranchDeclaration, ...]'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "canonical_branches",
  "owner": "stove0_protocol.BranchSetPlan",
  "unit": "member"
}
```

</details>
