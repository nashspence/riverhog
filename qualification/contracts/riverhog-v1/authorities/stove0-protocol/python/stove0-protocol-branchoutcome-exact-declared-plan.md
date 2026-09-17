# stove0_protocol.BranchOutcome.exact_declared_plan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-branchoutcome-exact-declared-plan:3d938d0bde -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-54b8edeeef"></a>
- <a id="s-65cab35c1c"></a>`distribution`: `stove0-protocol`
- <a id="s-afefea3973"></a>`module`: `stove0_protocol`
- <a id="s-7399116e0a"></a>`name`: `exact_declared_plan`
- <a id="s-2e49d70629"></a>`owner`: `stove0_protocol.BranchOutcome`
- <a id="s-647ae30eb7"></a>`unit`: `member`

### Declared structure

- <a id="s-a6b034b285"></a>`kind`: `"method"`
- <a id="s-1baeac658a"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [BranchOutcome](stove0-protocol-branchoutcome.md)

## Governing policies

- <a id="pa-a04b037691"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.BranchOutcome.exact_declared_plan`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5804c92b0aedd3ff40c16c2dd8c851fc1d7019939575f765aa66534580e25049 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "exact_declared_plan",
  "owner": "stove0_protocol.BranchOutcome",
  "unit": "member"
}
```

</details>
