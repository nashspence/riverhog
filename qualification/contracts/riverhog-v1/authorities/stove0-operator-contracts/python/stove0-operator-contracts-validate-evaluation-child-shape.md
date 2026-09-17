# stove0_operator_contracts.validate_evaluation_child_shape

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-validate-evalua-4db8ce4dac:f1928cf331 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1e5b9130dc"></a>
- <a id="s-8b12ba1d2c"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-29a519594b"></a>`module`: `stove0_operator_contracts`
- <a id="s-8cc19fbdf7"></a>`name`: `validate_evaluation_child_shape`
- <a id="s-7da86d3df7"></a>`unit`: `export`

### Declared structure

- <a id="s-066cef4355"></a>`kind`: `"function"`
- <a id="s-78fe37e1e8"></a>`signature`: `"\"(state: 'EvaluationChildState', output: 'OutputCollectionRef \| None') -> 'None'\""`

## Governing policies

- <a id="pa-f4bc83e91a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [reference/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.validate_evaluation_child_shape`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 57d34c7f4bab911a808ad723ade391155cf9386c39849ce8ff60d4d84022a286 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(state: 'EvaluationChildState', output: 'OutputCollectionRef | None') -> 'None'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "validate_evaluation_child_shape",
  "unit": "export"
}
```

</details>
