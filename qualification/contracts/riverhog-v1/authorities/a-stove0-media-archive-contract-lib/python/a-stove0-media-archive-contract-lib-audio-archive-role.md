# a_stove0_media_archive_contract_lib.AUDIO_ARCHIVE_ROLE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-contract-lib:a-stove0-media-archive-contract-lib-audio-c3b27c7f07:3d0500beaf -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-67b6b0b0bf"></a>
- <a id="s-3a1e9c7f91"></a>`distribution`: `a-stove0-media-archive-contract-lib`
- <a id="s-c53c0a8a4c"></a>`module`: `a_stove0_media_archive_contract_lib`
- <a id="s-f67d1afe77"></a>`name`: `AUDIO_ARCHIVE_ROLE`
- <a id="s-d2970adcaf"></a>`unit`: `export`

### Declared structure

- <a id="s-8dd6306073"></a>`kind`: `"constant"`
- <a id="s-8ad35addb5"></a>`value`: `"stove0.media.audio-archive/v1"`

## Governing policies

- <a id="pa-c2f111c5b2"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-contract-lib:a_stove0_media_archive_contract_lib](../../../evidence/sources/authorities.md#src-3f8c4376e0) — [some-implementations/stove0/targets/media-archive/contracts/src/a\_stove0\_media\_archive\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/contracts/src/a_stove0_media_archive_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_contract_lib.AUDIO_ARCHIVE_ROLE`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 12839d8a81e11909f6990a6d4f5c22e30f74dd291884ea731b18d33c381fb108 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0.media.audio-archive/v1"
  },
  "distribution": "a-stove0-media-archive-contract-lib",
  "module": "a_stove0_media_archive_contract_lib",
  "name": "AUDIO_ARCHIVE_ROLE",
  "unit": "export"
}
```

</details>
