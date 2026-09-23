# stove0_operator_contracts.parse_stove0_event

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-parse-stove0-event:3bc9b830e3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-280b6c3965"></a>
- <a id="s-58df574e68"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-0a3834d67a"></a>`module`: `stove0_operator_contracts`
- <a id="s-8dfc13abff"></a>`name`: `parse_stove0_event`
- <a id="s-7d85e7d3e7"></a>`unit`: `export`

### Declared structure

- <a id="s-8063455461"></a>`kind`: `"function"`
- <a id="s-5338c009f1"></a>`signature`: `"\"(value: 'object') -> 'Stove0LifecycleEvent'\""`

## Governing policies

- <a id="pa-77c76f75a1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.parse_stove0_event`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 75dd0a0a300dfc2dd69fafbb5532298847f79ba4c0135a618eee110b62fc593a -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'object') -> 'Stove0LifecycleEvent'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "parse_stove0_event",
  "unit": "export"
}
```

</details>
