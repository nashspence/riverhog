# stove0_operator_contracts.EvaluationUpdatedEventData.__getitem__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-evaluationupdat-832974db51:e810a1b8b2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-53fb5c1733"></a>
- <a id="s-22c6207146"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-1e62c38b00"></a>`module`: `stove0_operator_contracts`
- <a id="s-52cc6b4b3c"></a>`name`: `__getitem__`
- <a id="s-0dbc04c876"></a>`owner`: `stove0_operator_contracts.EvaluationUpdatedEventData`
- <a id="s-0bb28a9187"></a>`unit`: `member`

### Declared structure

- <a id="s-1b8d64d714"></a>`kind`: `"method"`
- <a id="s-9da1f059a3"></a>`signature`: `"\"(self, key: 'str') -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [EvaluationUpdatedEventData](stove0-operator-contracts-evaluationupdatedeventdata.md)

## Governing policies

- <a id="pa-eb9a8c028e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.EvaluationUpdatedEventData.__getitem__`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1b51ced7165dcff6d2df822c32ba2907f881f044996a216d2334fe09d882334c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str') -> 'Any'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "__getitem__",
  "owner": "stove0_operator_contracts.EvaluationUpdatedEventData",
  "unit": "member"
}
```

</details>
