# stove0_operator_contracts.EvaluationCreatedEvent.validate_time

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-evaluationcreat-728864284b:d974c4b8e5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a099899769"></a>
- <a id="s-a411f5cc13"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-d04e899f60"></a>`module`: `stove0_operator_contracts`
- <a id="s-d62575d0d3"></a>`name`: `validate_time`
- <a id="s-82ad270f27"></a>`owner`: `stove0_operator_contracts.EvaluationCreatedEvent`
- <a id="s-8a71b38278"></a>`unit`: `member`

### Declared structure

- <a id="s-74da270ae3"></a>`kind`: `"classmethod"`
- <a id="s-4c01e69fb1"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [EvaluationCreatedEvent](stove0-operator-contracts-evaluationcreatedevent.md)

## Governing policies

- <a id="pa-23467475fa"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.EvaluationCreatedEvent.validate_time`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2d918f5ef3063bb237dd4453c5cc8a377dc36a67d68832c0a7b43d31f377e902 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "validate_time",
  "owner": "stove0_operator_contracts.EvaluationCreatedEvent",
  "unit": "member"
}
```

</details>
