# stove0_operator_contracts.validate_evaluation_state_shape

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-validate-evalua-529da0acaf:2634fa1d5a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ac4cabdc7b"></a>
- <a id="s-9e61c0c29f"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-f1cd3bf4c2"></a>`module`: `stove0_operator_contracts`
- <a id="s-13e04aa0cb"></a>`name`: `validate_evaluation_state_shape`
- <a id="s-ca599ffd8d"></a>`unit`: `export`

### Declared structure

- <a id="s-a05e9255c9"></a>`kind`: `"function"`
- <a id="s-268fef6fe4"></a>`signature`: `"\"(definition: 'EvaluationDefinition', children: 'Sequence[object]', reviews: 'Sequence[object]') -> 'None'\""`

## Governing policies

- <a id="pa-e75006c69e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.validate_evaluation_state_shape`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7279a29c6233cdc7eed1ac6ca8eeae3150423044aee87ef93d4ada02ae4811c1 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(definition: 'EvaluationDefinition', children: 'Sequence[object]', reviews: 'Sequence[object]') -> 'None'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "validate_evaluation_state_shape",
  "unit": "export"
}
```
