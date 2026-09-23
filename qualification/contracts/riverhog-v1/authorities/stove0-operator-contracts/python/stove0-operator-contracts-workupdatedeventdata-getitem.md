# stove0_operator_contracts.WorkUpdatedEventData.__getitem__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workupdatedeven-efa2f2174f:a749ee0c37 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-700d3c63f8"></a>
- <a id="s-8f88c1cc56"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-c9f3f2a7dd"></a>`module`: `stove0_operator_contracts`
- <a id="s-5b06b0e539"></a>`name`: `__getitem__`
- <a id="s-7e00b8bc24"></a>`owner`: `stove0_operator_contracts.WorkUpdatedEventData`
- <a id="s-e226cec58a"></a>`unit`: `member`

### Declared structure

- <a id="s-5722a4c2f5"></a>`kind`: `"method"`
- <a id="s-6173b74194"></a>`signature`: `"\"(self, key: 'str') -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [WorkUpdatedEventData](stove0-operator-contracts-workupdatedeventdata.md)

## Governing policies

- <a id="pa-a72f323634"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkUpdatedEventData.__getitem__`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5c0258e08258abbb241de97b738222aa265533d69ded251d40eb68ef8be829c2 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str') -> 'Any'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "__getitem__",
  "owner": "stove0_operator_contracts.WorkUpdatedEventData",
  "unit": "member"
}
```

</details>
