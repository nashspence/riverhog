# stove0_observer_protocol.accept_observation_result

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-accept-observation-result:dfaf439e31 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6e34e05de8"></a>
- <a id="s-9a02fbee63"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-14bf247ed3"></a>`module`: `stove0_observer_protocol`
- <a id="s-5bf2014b92"></a>`name`: `accept_observation_result`
- <a id="s-70e93568e4"></a>`unit`: `export`

### Declared structure

- <a id="s-10562f83f1"></a>`kind`: `"function"`
- <a id="s-238e5e0e27"></a>`signature`: `"\"(result: 'ContentObservationResult', request: 'ContentObservationRequest', descriptor: 'ObserverDescriptor', semantic_validators: 'SemanticValidatorProvider \| None' = None) -> 'None'\""`

## Governing policies

- <a id="pa-09b3ad0278"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.accept_observation_result`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5336a1b70c6be6f18405cfb2b78d360eb995cfd6f7c803bd5367cf63e22d9203 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(result: 'ContentObservationResult', request: 'ContentObservationRequest', descriptor: 'ObserverDescriptor', semantic_validators: 'SemanticValidatorProvider | None' = None) -> 'None'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "accept_observation_result",
  "unit": "export"
}
```

</details>
