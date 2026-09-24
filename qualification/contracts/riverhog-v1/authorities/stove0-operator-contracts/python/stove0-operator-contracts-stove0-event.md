# stove0_operator_contracts.stove0_event

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-stove0-event:09fb3c0f65 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-949ed954c8"></a>
- <a id="s-aa7529359f"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-c4167ddcbe"></a>`module`: `stove0_operator_contracts`
- <a id="s-ac6fd6a2e0"></a>`name`: `stove0_event`
- <a id="s-01a4d22fec"></a>`unit`: `export`

### Declared structure

- <a id="s-f79256aa4a"></a>`kind`: `"function"`
- <a id="s-ede717616c"></a>`signature`: `"\"(*, type: 'Stove0EventType', subject: 'str', payload: 'Mapping[str, Any]') -> 'Stove0LifecycleEvent'\""`

## Governing policies

- <a id="pa-c639f0be7c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.stove0_event`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5bfbe3138192dca5938a9d32a3f5c3b913c12ffb814ad8eddbc9ca03bc27c172 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, type: 'Stove0EventType', subject: 'str', payload: 'Mapping[str, Any]') -> 'Stove0LifecycleEvent'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "stove0_event",
  "unit": "export"
}
```

</details>
