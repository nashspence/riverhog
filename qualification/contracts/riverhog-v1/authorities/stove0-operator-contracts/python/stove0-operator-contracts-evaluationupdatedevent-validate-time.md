# stove0_operator_contracts.EvaluationUpdatedEvent.validate_time

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-evaluationupdat-1cc3a6c6c6:3329c5835a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f65c0089d3"></a>
- <a id="s-a7d1b75eec"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-b2c0a49fc8"></a>`module`: `stove0_operator_contracts`
- <a id="s-345ee86851"></a>`name`: `validate_time`
- <a id="s-58f6bd6b3d"></a>`owner`: `stove0_operator_contracts.EvaluationUpdatedEvent`
- <a id="s-52f2feb2b9"></a>`unit`: `member`

### Declared structure

- <a id="s-709bd1a38e"></a>`kind`: `"classmethod"`
- <a id="s-9bc229feae"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [EvaluationUpdatedEvent](stove0-operator-contracts-evaluationupdatedevent.md)

## Governing policies

- <a id="pa-4ed3b3f95f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.EvaluationUpdatedEvent.validate_time`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9cdc2c013ba8bc9ead34d1f9e2046a92c77fd0213972e94c90b3920e6c458ab8 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "validate_time",
  "owner": "stove0_operator_contracts.EvaluationUpdatedEvent",
  "unit": "member"
}
```

</details>
