# stove0_operator_contracts.EvaluationView.exact_identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-evaluationview-c8e434f506:b0ef064394 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-68911bcf93"></a>
- <a id="s-56a382f29a"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-122c2c9bbc"></a>`module`: `stove0_operator_contracts`
- <a id="s-cf322ee591"></a>`name`: `exact_identity`
- <a id="s-88d6a1e5fe"></a>`owner`: `stove0_operator_contracts.EvaluationView`
- <a id="s-db3b4e30c6"></a>`unit`: `member`

### Declared structure

- <a id="s-4d8f497501"></a>`kind`: `"method"`
- <a id="s-85d20bb6c3"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [EvaluationView](stove0-operator-contracts-evaluationview.md)

## Governing policies

- <a id="pa-dc24283b5b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [reference/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.EvaluationView.exact_identity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 42c3e39b12e739966055475ab8aa17e6408d68a89b1d2cf9e7453691d969f9ca -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "exact_identity",
  "owner": "stove0_operator_contracts.EvaluationView",
  "unit": "member"
}
```

</details>
