# stove0_operator_contracts.EvaluationUpdatedEventData.get

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-evaluationupdat-c2e6aad642:4647856e8a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ce5f6ef4f6"></a>
- <a id="s-6732fc1603"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-ac593adba7"></a>`module`: `stove0_operator_contracts`
- <a id="s-e5970b9301"></a>`name`: `get`
- <a id="s-0e6ca1ae03"></a>`owner`: `stove0_operator_contracts.EvaluationUpdatedEventData`
- <a id="s-526b265dbd"></a>`unit`: `member`

### Declared structure

- <a id="s-d31bfdcf5a"></a>`kind`: `"method"`
- <a id="s-edfcf132c8"></a>`signature`: `"\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [EvaluationUpdatedEventData](stove0-operator-contracts-evaluationupdatedeventdata.md)

## Governing policies

- <a id="pa-363b2f49f7"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.EvaluationUpdatedEventData.get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 06f6c703d9299e2e169ac894ac7fe7f5a9eae1c902f0a74c79569c5889f98212 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "get",
  "owner": "stove0_operator_contracts.EvaluationUpdatedEventData",
  "unit": "member"
}
```

</details>
