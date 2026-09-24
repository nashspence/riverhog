# a_stove0_media_sampling_contract_lib.validate_media_sampling_observation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-sampling-contract-lib:a-stove0-media-sampling-contract-lib-vali-49cc320fa6:c568137d51 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-sampling-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-edc591bdd3"></a>
- <a id="s-5fbaa20aff"></a>`distribution`: `a-stove0-media-sampling-contract-lib`
- <a id="s-d2e87ba922"></a>`module`: `a_stove0_media_sampling_contract_lib`
- <a id="s-172fac34cd"></a>`name`: `validate_media_sampling_observation`
- <a id="s-e6c29ee209"></a>`unit`: `export`

### Declared structure

- <a id="s-7802d78396"></a>`kind`: `"function"`
- <a id="s-108b095973"></a>`signature`: `"\"(request: 'ContentObservationRequest', facts: 'Mapping[str, object]') -> 'None'\""`

## Governing policies

- <a id="pa-20d538782d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-sampling-contract-lib:a_stove0_media_sampling_contract_lib](../../../evidence/sources/authorities.md#src-68256911b9) — [some-implementations/stove0/observers/contracts/media-sampling/src/a\_stove0\_media\_sampling\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/observers/contracts/media-sampling/src/a_stove0_media_sampling_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_sampling_contract_lib.validate_media_sampling_observation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b6317bee5c629b320e314a53929c6f98d23bb357687e55767b7b8e7ebbdb87fc -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(request: 'ContentObservationRequest', facts: 'Mapping[str, object]') -> 'None'\""
  },
  "distribution": "a-stove0-media-sampling-contract-lib",
  "module": "a_stove0_media_sampling_contract_lib",
  "name": "validate_media_sampling_observation",
  "unit": "export"
}
```

</details>
