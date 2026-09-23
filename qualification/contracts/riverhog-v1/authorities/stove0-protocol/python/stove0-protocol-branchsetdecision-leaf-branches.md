# stove0_protocol.BranchSetDecision.leaf_branches

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-branchsetdecision-leaf-branches:1463dc948b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-550caf4d79"></a>
- <a id="s-7873ecebaf"></a>`distribution`: `stove0-protocol`
- <a id="s-b48428990a"></a>`module`: `stove0_protocol`
- <a id="s-b282f8e68a"></a>`name`: `leaf_branches`
- <a id="s-8742c9d896"></a>`owner`: `stove0_protocol.BranchSetDecision`
- <a id="s-25d68fe093"></a>`unit`: `member`

### Declared structure

- <a id="s-c8bf7c10d1"></a>`kind`: `"method"`
- <a id="s-f3c4b47cf0"></a>`signature`: `"\"(self) -> 'tuple[BranchPlan, ...]'\""`

## Maintained corroboration

### Related interface records

- [BranchSetDecision](stove0-protocol-branchsetdecision.md)

## Governing policies

- <a id="pa-8f51649ab1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.BranchSetDecision.leaf_branches`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
