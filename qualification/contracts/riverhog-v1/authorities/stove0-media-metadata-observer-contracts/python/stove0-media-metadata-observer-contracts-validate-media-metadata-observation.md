# stove0_media_metadata_observer_contracts.validate_media_metadata_observation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-metadata-observer-contracts:stove0-media-metadata-observer-contracts-b92479b02d:f7c82941b9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-metadata-observer-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b38b10edf4"></a>
- <a id="s-05be943411"></a>`distribution`: `stove0-media-metadata-observer-contracts`
- <a id="s-c68672e22b"></a>`module`: `stove0_media_metadata_observer_contracts`
- <a id="s-151c95e971"></a>`name`: `validate_media_metadata_observation`
- <a id="s-9ab7a8e26d"></a>`unit`: `export`

### Declared structure

- <a id="s-1f435129ec"></a>`kind`: `"function"`
- <a id="s-d42dbb4859"></a>`signature`: `"\"(request: 'ObservationRequest', facts: 'Mapping[str, object]') -> 'None'\""`

## Governing policies

- <a id="pa-38f9d2f0d1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-media-metadata-observer-contracts:stove0_media_metadata_observer_contracts](../../../evidence/sources.md#src-8d1649a0f1) — [reference/stove0/observers/contracts/media-metadata/src/stove0\_media\_metadata\_observer\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/observers/contracts/media-metadata/src/stove0_media_metadata_observer_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_media_metadata_observer_contracts.validate_media_metadata_observation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 869f640a76f538c36b40caea67bd823d9e9ed0b4c796b8a892d4661ff6af1360 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(request: 'ObservationRequest', facts: 'Mapping[str, object]') -> 'None'\""
  },
  "distribution": "stove0-media-metadata-observer-contracts",
  "module": "stove0_media_metadata_observer_contracts",
  "name": "validate_media_metadata_observation",
  "unit": "export"
}
```

</details>
