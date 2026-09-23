# stove0_operator_contracts.EvaluationView.from_record

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-evaluationview-from-record:fecf36b83f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9523845453"></a>
- <a id="s-6aa158af51"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-36483db858"></a>`module`: `stove0_operator_contracts`
- <a id="s-9b64438131"></a>`name`: `from_record`
- <a id="s-3f7dda719f"></a>`owner`: `stove0_operator_contracts.EvaluationView`
- <a id="s-f8fff9bf8c"></a>`unit`: `member`

### Declared structure

- <a id="s-e7df48c170"></a>`kind`: `"classmethod"`
- <a id="s-57673fff90"></a>`signature`: `"\"(cls, record: 'BaseModel \| Mapping[str, Any]') -> 'EvaluationView'\""`

## Maintained corroboration

### Related interface records

- [EvaluationView](stove0-operator-contracts-evaluationview.md)

## Governing policies

- <a id="pa-3b65b586af"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.EvaluationView.from_record`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fc606349fa8e2ebe59f053f753d96fef572017575f407fa25643e608d6788f75 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, record: 'BaseModel | Mapping[str, Any]') -> 'EvaluationView'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "from_record",
  "owner": "stove0_operator_contracts.EvaluationView",
  "unit": "member"
}
```

</details>
