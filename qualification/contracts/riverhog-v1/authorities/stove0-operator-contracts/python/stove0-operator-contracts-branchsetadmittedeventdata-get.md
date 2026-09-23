# stove0_operator_contracts.BranchSetAdmittedEventData.get

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-branchsetadmitt-6cb2ef5ce5:585cee982f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9b267c0190"></a>
- <a id="s-9bbbe53cc5"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-d40cbbcf42"></a>`module`: `stove0_operator_contracts`
- <a id="s-feca1bd73b"></a>`name`: `get`
- <a id="s-8b84e88fc0"></a>`owner`: `stove0_operator_contracts.BranchSetAdmittedEventData`
- <a id="s-275455b6cd"></a>`unit`: `member`

### Declared structure

- <a id="s-c57cf58082"></a>`kind`: `"method"`
- <a id="s-e334763d99"></a>`signature`: `"\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [BranchSetAdmittedEventData](stove0-operator-contracts-branchsetadmittedeventdata.md)

## Governing policies

- <a id="pa-badbe3fadd"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.BranchSetAdmittedEventData.get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c3ca42b3a7eb00aafaf63d76aea29989d7ee69aa625886546b876c1f2a314373 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "get",
  "owner": "stove0_operator_contracts.BranchSetAdmittedEventData",
  "unit": "member"
}
```

</details>
