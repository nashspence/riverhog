# stove0_operator_contracts.Stove0EventData.get

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-stove0eventdata-get:ca20af1421 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6ff33d52c4"></a>
- <a id="s-2fe920398e"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-792487b900"></a>`module`: `stove0_operator_contracts`
- <a id="s-d2c171809f"></a>`name`: `get`
- <a id="s-cedfc2f773"></a>`owner`: `stove0_operator_contracts.Stove0EventData`
- <a id="s-b94f67a4cf"></a>`unit`: `member`

### Declared structure

- <a id="s-f29c78bdba"></a>`kind`: `"method"`
- <a id="s-42488a5ab1"></a>`signature`: `"\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [Stove0EventData](stove0-operator-contracts-stove0eventdata.md)

## Governing policies

- <a id="pa-19f4581f5d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [reference/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.Stove0EventData.get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 781eb1b070a17ff06d56b8a36aca8ec831c2bb964451faff55dca895b215f850 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "get",
  "owner": "stove0_operator_contracts.Stove0EventData",
  "unit": "member"
}
```

</details>
