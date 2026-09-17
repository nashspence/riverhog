# stove0_operator_contracts.WorkCreatedEventData.__getitem__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workcreatedeven-bf2fd05165:9a62888727 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-80e6f19789"></a>
- <a id="s-e1890a2a1f"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-1051ba7220"></a>`module`: `stove0_operator_contracts`
- <a id="s-4169bfc84e"></a>`name`: `__getitem__`
- <a id="s-c9f47cef83"></a>`owner`: `stove0_operator_contracts.WorkCreatedEventData`
- <a id="s-1ebc9e82fe"></a>`unit`: `member`

### Declared structure

- <a id="s-7b6312101a"></a>`kind`: `"method"`
- <a id="s-79a6e25ad3"></a>`signature`: `"\"(self, key: 'str') -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [WorkCreatedEventData](stove0-operator-contracts-workcreatedeventdata.md)

## Governing policies

- <a id="pa-5e4be3beb5"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [reference/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkCreatedEventData.__getitem__`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 42d1559a0c4b2c842f15c2deaa2ed4f5919e9d5f6c923c0fd08a601f208fd926 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str') -> 'Any'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "__getitem__",
  "owner": "stove0_operator_contracts.WorkCreatedEventData",
  "unit": "member"
}
```

</details>
