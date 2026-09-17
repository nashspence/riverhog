# stove0_operator_contracts.validate_evaluation_review_shape

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-validate-evalua-d19783309e:1e624a66e0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b4fc160a6d"></a>
- <a id="s-929807faaa"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-f05964176f"></a>`module`: `stove0_operator_contracts`
- <a id="s-e7b6c43aaa"></a>`name`: `validate_evaluation_review_shape`
- <a id="s-68fdfa29b6"></a>`unit`: `export`

### Declared structure

- <a id="s-932ae8aa88"></a>`kind`: `"function"`
- <a id="s-d501a58503"></a>`signature`: `"\"(rating: 'int \| None', note: 'str \| None') -> 'None'\""`

## Governing policies

- <a id="pa-2ea69cff68"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — [reference/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.validate_evaluation_review_shape`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5b0370b379aed9ccf3199d436094c29fce7d6cc95bf30a0a52b2d9a1e2204f3d -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(rating: 'int | None', note: 'str | None') -> 'None'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "validate_evaluation_review_shape",
  "unit": "export"
}
```

</details>
