# stove0_operator_contracts.WorkUpdatedEventData.get

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workupdatedeventdata-get:6fd7033ef9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1eb05806ea"></a>
- <a id="s-6cac70d618"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-404fc16c0d"></a>`module`: `stove0_operator_contracts`
- <a id="s-7950a1c310"></a>`name`: `get`
- <a id="s-640ef91dba"></a>`owner`: `stove0_operator_contracts.WorkUpdatedEventData`
- <a id="s-bcbb9e9ac7"></a>`unit`: `member`

### Declared structure

- <a id="s-7ebcacbec4"></a>`kind`: `"method"`
- <a id="s-daaecd7525"></a>`signature`: `"\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [WorkUpdatedEventData](stove0-operator-contracts-workupdatedeventdata.md)

## Governing policies

- <a id="pa-74a3441666"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkUpdatedEventData.get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3c1d314bb8f7b6eef856057589054f9e37360466b20f395b73a431dc84391768 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "get",
  "owner": "stove0_operator_contracts.WorkUpdatedEventData",
  "unit": "member"
}
```

</details>
