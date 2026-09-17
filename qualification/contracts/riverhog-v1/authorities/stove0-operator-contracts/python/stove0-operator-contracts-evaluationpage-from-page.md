# stove0_operator_contracts.EvaluationPage.from_page

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-evaluationpage-from-page:41da6ef749 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5d45cae2ae"></a>
- <a id="s-1c1419a6bf"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-9421effef9"></a>`module`: `stove0_operator_contracts`
- <a id="s-4224e338b0"></a>`name`: `from_page`
- <a id="s-cc325bba34"></a>`owner`: `stove0_operator_contracts.EvaluationPage`
- <a id="s-4a960cb6f6"></a>`unit`: `member`

### Declared structure

- <a id="s-06b40b769d"></a>`kind`: `"classmethod"`
- <a id="s-f40fb1d0d8"></a>`signature`: `"\"(cls, page: 'Mapping[str, Any]') -> 'EvaluationPage'\""`

## Maintained corroboration

### Related interface records

- [EvaluationPage](stove0-operator-contracts-evaluationpage.md)

## Governing policies

- <a id="pa-ae170fff0d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [reference/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.EvaluationPage.from_page`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7dc18607dcd58d389d2ff0a05a30f0c651551bd93bf54918edcc23c02d83ae81 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, page: 'Mapping[str, Any]') -> 'EvaluationPage'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "from_page",
  "owner": "stove0_operator_contracts.EvaluationPage",
  "unit": "member"
}
```

</details>
