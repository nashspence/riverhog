# a_stove0_media_metadata_contract_lib.validate_media_metadata_observation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-metadata-contract-lib:a-stove0-media-metadata-contract-lib-vali-afa6c6583a:e7bddb9bd0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-metadata-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f18b0524bc"></a>
- <a id="s-7962956362"></a>`distribution`: `a-stove0-media-metadata-contract-lib`
- <a id="s-9a7a0bfe56"></a>`module`: `a_stove0_media_metadata_contract_lib`
- <a id="s-78fdc43a10"></a>`name`: `validate_media_metadata_observation`
- <a id="s-474c53da9d"></a>`unit`: `export`

### Declared structure

- <a id="s-60eaf46dc7"></a>`kind`: `"function"`
- <a id="s-2f94389ea7"></a>`signature`: `"\"(request: 'ContentObservationRequest', facts: 'Mapping[str, object]') -> 'None'\""`

## Governing policies

- <a id="pa-f50abf0adc"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-metadata-contract-lib:a_stove0_media_metadata_contract_lib](../../../evidence/sources/authorities.md#src-9ad9aed69d) — [some-implementations/stove0/observers/contracts/media-metadata/src/a\_stove0\_media\_metadata\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/observers/contracts/media-metadata/src/a_stove0_media_metadata_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_metadata_contract_lib.validate_media_metadata_observation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5f4abd260fa2dbeced6717eee2641dfee6141db5d6df2d616fe16b2b55adb715 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(request: 'ContentObservationRequest', facts: 'Mapping[str, object]') -> 'None'\""
  },
  "distribution": "a-stove0-media-metadata-contract-lib",
  "module": "a_stove0_media_metadata_contract_lib",
  "name": "validate_media_metadata_observation",
  "unit": "export"
}
```

</details>
