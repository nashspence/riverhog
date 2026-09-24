# a_stove0_media_archive_contract_lib.validate_audio_archive_intent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-contract-lib:a-stove0-media-archive-contract-lib-valid-8b06809afc:0f5bc56c36 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-79803f8011"></a>
- <a id="s-0551fd13c3"></a>`distribution`: `a-stove0-media-archive-contract-lib`
- <a id="s-3cdda8894e"></a>`module`: `a_stove0_media_archive_contract_lib`
- <a id="s-d61c9381a5"></a>`name`: `validate_audio_archive_intent`
- <a id="s-e71ae43593"></a>`unit`: `export`

### Declared structure

- <a id="s-d0296b4d43"></a>`kind`: `"function"`
- <a id="s-82935c0352"></a>`signature`: `"\"(intent: 'Mapping[str, object]') -> 'None'\""`

## Governing policies

- <a id="pa-48feeeb80d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-contract-lib:a_stove0_media_archive_contract_lib](../../../evidence/sources/authorities.md#src-3f8c4376e0) — [some-implementations/stove0/targets/media-archive/contracts/src/a\_stove0\_media\_archive\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/contracts/src/a_stove0_media_archive_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_contract_lib.validate_audio_archive_intent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 96da626d1c18d757d856924a0cb163b805bc92140e9778157e0a8f837ef1d445 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(intent: 'Mapping[str, object]') -> 'None'\""
  },
  "distribution": "a-stove0-media-archive-contract-lib",
  "module": "a_stove0_media_archive_contract_lib",
  "name": "validate_audio_archive_intent",
  "unit": "export"
}
```

</details>
