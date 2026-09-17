# stove0_operator_contracts.Stove0EventData.__getitem__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-stove0eventdata-getitem:162fe3c671 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-32f28c588b"></a>
- <a id="s-090cb8f6f4"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-abb792a744"></a>`module`: `stove0_operator_contracts`
- <a id="s-95fff9a354"></a>`name`: `__getitem__`
- <a id="s-c980b1c1b1"></a>`owner`: `stove0_operator_contracts.Stove0EventData`
- <a id="s-aef469b37c"></a>`unit`: `member`

### Declared structure

- <a id="s-12d1e81b6f"></a>`kind`: `"method"`
- <a id="s-abd5a2fde5"></a>`signature`: `"\"(self, key: 'str') -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [Stove0EventData](stove0-operator-contracts-stove0eventdata.md)

## Governing policies

- <a id="pa-e479056dd5"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [reference/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.Stove0EventData.__getitem__`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f0dbac4c4732e6c71fe0bbf2ec32c183f5cff05a5ab648a84bcafbcedbef270e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str') -> 'Any'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "__getitem__",
  "owner": "stove0_operator_contracts.Stove0EventData",
  "unit": "member"
}
```

</details>
