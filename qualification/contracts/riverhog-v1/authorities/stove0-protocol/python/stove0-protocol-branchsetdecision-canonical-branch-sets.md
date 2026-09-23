# stove0_protocol.BranchSetDecision.canonical_branch_sets

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-branchsetdecision-canonic-091e4c6864:d1269ffa33 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c84f9860a7"></a>
- <a id="s-744627ddb9"></a>`distribution`: `stove0-protocol`
- <a id="s-fcfabeb141"></a>`module`: `stove0_protocol`
- <a id="s-92f00ed34e"></a>`name`: `canonical_branch_sets`
- <a id="s-707ce81567"></a>`owner`: `stove0_protocol.BranchSetDecision`
- <a id="s-5b4b9f4c20"></a>`unit`: `member`

### Declared structure

- <a id="s-dec0aa48b8"></a>`kind`: `"classmethod"`
- <a id="s-204eac8275"></a>`signature`: `"\"(cls, value: 'tuple[BranchSetPlan, ...]') -> 'tuple[BranchSetPlan, ...]'\""`

## Maintained corroboration

### Related interface records

- [BranchSetDecision](stove0-protocol-branchsetdecision.md)

## Governing policies

- <a id="pa-94320d308b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.BranchSetDecision.canonical_branch_sets`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e31bb4113614978a62f2154d527749e77c8f9a8bdb9004e58312257a6dd2b5bb -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[BranchSetPlan, ...]') -> 'tuple[BranchSetPlan, ...]'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "canonical_branch_sets",
  "owner": "stove0_protocol.BranchSetDecision",
  "unit": "member"
}
```

</details>
