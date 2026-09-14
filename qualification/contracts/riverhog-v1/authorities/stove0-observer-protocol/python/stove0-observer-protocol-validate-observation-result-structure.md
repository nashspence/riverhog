# stove0_observer_protocol.validate_observation_result_structure

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-validate-observa-c3b63555d7:8a246b0172 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9d8ef79bc8"></a>
- <a id="s-8342cabd74"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-b8c7e18824"></a>`module`: `stove0_observer_protocol`
- <a id="s-682fd5c4b4"></a>`name`: `validate_observation_result_structure`
- <a id="s-f34cca3302"></a>`unit`: `export`

### Declared structure

- <a id="s-ff5baf34b7"></a>`kind`: `"function"`
- <a id="s-2bb41eabdd"></a>`signature`: `"\"(result: 'ObservationResult', request: 'ObservationRequest', descriptor: 'ObserverDescriptor') -> 'ObserverContractSupport'\""`

## Governing policies

- <a id="pa-1fd4e5f9e2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.validate_observation_result_structure`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 933a4faf1a3bb3efd51d4532dafa349530cde73567df2bc65fa7d7682896ab7c -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(result: 'ObservationResult', request: 'ObservationRequest', descriptor: 'ObserverDescriptor') -> 'ObserverContractSupport'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "validate_observation_result_structure",
  "unit": "export"
}
```
