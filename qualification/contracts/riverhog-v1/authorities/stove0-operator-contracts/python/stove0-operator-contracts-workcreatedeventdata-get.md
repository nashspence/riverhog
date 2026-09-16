# stove0_operator_contracts.WorkCreatedEventData.get

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workcreatedeventdata-get:04c62aa745 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4b9f8c812e"></a>
- <a id="s-0a28009dc5"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-487933ca61"></a>`module`: `stove0_operator_contracts`
- <a id="s-5838a144d2"></a>`name`: `get`
- <a id="s-d79ec0a769"></a>`owner`: `stove0_operator_contracts.WorkCreatedEventData`
- <a id="s-a468ca8758"></a>`unit`: `member`

### Declared structure

- <a id="s-ef2f6d5bc4"></a>`kind`: `"method"`
- <a id="s-a34cd9a816"></a>`signature`: `"\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [WorkCreatedEventData](stove0-operator-contracts-workcreatedeventdata.md)

## Governing policies

- <a id="pa-2dac0976d1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkCreatedEventData.get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 764095a83a97155e64e84575282eb7fdcf696e978fce89ba6f109d922de75f49 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "get",
  "owner": "stove0_operator_contracts.WorkCreatedEventData",
  "unit": "member"
}
```

</details>
