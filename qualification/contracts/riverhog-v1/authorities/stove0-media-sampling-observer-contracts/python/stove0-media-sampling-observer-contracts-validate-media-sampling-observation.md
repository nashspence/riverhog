# stove0_media_sampling_observer_contracts.validate_media_sampling_observation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-sampling-observer-contracts:stove0-media-sampling-observer-contracts-79b6ffd88d:c4695be24b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-sampling-observer-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7032d8e868"></a>
- <a id="s-ac86f6d51f"></a>`distribution`: `stove0-media-sampling-observer-contracts`
- <a id="s-7d207a412b"></a>`module`: `stove0_media_sampling_observer_contracts`
- <a id="s-bc0efbafc1"></a>`name`: `validate_media_sampling_observation`
- <a id="s-04d1de84ec"></a>`unit`: `export`

### Declared structure

- <a id="s-9d39a5884f"></a>`kind`: `"function"`
- <a id="s-9a8656f493"></a>`signature`: `"\"(request: 'ObservationRequest', facts: 'Mapping[str, object]') -> 'None'\""`

## Governing policies

- <a id="pa-c6714ef04d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-media-sampling-observer-contracts:stove0_media_sampling_observer_contracts](../../../evidence/sources/authorities.md#src-b9344d06d7) — [reference/stove0/observers/contracts/media-sampling/src/stove0\_media\_sampling\_observer\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/observers/contracts/media-sampling/src/stove0_media_sampling_observer_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_media_sampling_observer_contracts.validate_media_sampling_observation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 33a6169c43b3c5f182a1c76394713727cc7decad36d5759dffb84b6be87a911c -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(request: 'ObservationRequest', facts: 'Mapping[str, object]') -> 'None'\""
  },
  "distribution": "stove0-media-sampling-observer-contracts",
  "module": "stove0_media_sampling_observer_contracts",
  "name": "validate_media_sampling_observation",
  "unit": "export"
}
```

</details>
