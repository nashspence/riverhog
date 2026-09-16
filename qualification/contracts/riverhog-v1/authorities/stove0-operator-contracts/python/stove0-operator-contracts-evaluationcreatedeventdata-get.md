# stove0_operator_contracts.EvaluationCreatedEventData.get

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-evaluationcreat-3a5642dbc6:d344393080 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-22d9a4ebd1"></a>
- <a id="s-02f5b4fc9b"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-c2d5857517"></a>`module`: `stove0_operator_contracts`
- <a id="s-59c7771199"></a>`name`: `get`
- <a id="s-f28955ca28"></a>`owner`: `stove0_operator_contracts.EvaluationCreatedEventData`
- <a id="s-04a9891959"></a>`unit`: `member`

### Declared structure

- <a id="s-b446718c82"></a>`kind`: `"method"`
- <a id="s-a3efc52142"></a>`signature`: `"\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [EvaluationCreatedEventData](stove0-operator-contracts-evaluationcreatedeventdata.md)

## Governing policies

- <a id="pa-777aec02e7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.EvaluationCreatedEventData.get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 12c78adce37ef7e55319c7e27573e407f790f0a5795d2e3b9d4b6d3e174b703d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "get",
  "owner": "stove0_operator_contracts.EvaluationCreatedEventData",
  "unit": "member"
}
```
