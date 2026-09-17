# stove0_operator_contracts.WorkCreatedEvent.exact_subject

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workcreatedeven-a19ffffa39:7dfe541fdd -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-265fb0e717"></a>
- <a id="s-f6312db9e9"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-c15be7988e"></a>`module`: `stove0_operator_contracts`
- <a id="s-2f23dc7d23"></a>`name`: `exact_subject`
- <a id="s-5c96355bdd"></a>`owner`: `stove0_operator_contracts.WorkCreatedEvent`
- <a id="s-7fb9010256"></a>`unit`: `member`

### Declared structure

- <a id="s-77179220a1"></a>`kind`: `"method"`
- <a id="s-8e8600ec63"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [WorkCreatedEvent](stove0-operator-contracts-workcreatedevent.md)

## Governing policies

- <a id="pa-f5884793ce"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [reference/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkCreatedEvent.exact_subject`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 25db5647f4e6a26174046f083a09b062212547e09b2c8038bc34c268f0830b4f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "exact_subject",
  "owner": "stove0_operator_contracts.WorkCreatedEvent",
  "unit": "member"
}
```

</details>
