# a_stove0_media_archive_contract_lib.AUDIO_ARCHIVE_INTENT_SEMANTICS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-contract-lib:a-stove0-media-archive-contract-lib-audio-f82ac112c9:3de0784386 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9c2353bd19"></a>
- <a id="s-fd708938ea"></a>`distribution`: `a-stove0-media-archive-contract-lib`
- <a id="s-be38fd3f35"></a>`module`: `a_stove0_media_archive_contract_lib`
- <a id="s-8e05d8191d"></a>`name`: `AUDIO_ARCHIVE_INTENT_SEMANTICS`
- <a id="s-5a3aeb6bf8"></a>`unit`: `export`

### Declared structure

- <a id="s-d2bd2405eb"></a>`kind`: `"object"`
- <a id="s-c3d001b660"></a>`type`: `"stove0_protocol.models.SemanticValidationProfile"`

## Governing policies

- <a id="pa-f66aa44ae2"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-contract-lib:a_stove0_media_archive_contract_lib](../../../evidence/sources/authorities.md#src-3f8c4376e0) — [some-implementations/stove0/targets/media-archive/contracts/src/a\_stove0\_media\_archive\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/contracts/src/a_stove0_media_archive_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_contract_lib.AUDIO_ARCHIVE_INTENT_SEMANTICS`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b45212b424908aba9b78d2e4ba2653a92a8565bb1121532afdf2b018e170b9d0 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "stove0_protocol.models.SemanticValidationProfile"
  },
  "distribution": "a-stove0-media-archive-contract-lib",
  "module": "a_stove0_media_archive_contract_lib",
  "name": "AUDIO_ARCHIVE_INTENT_SEMANTICS",
  "unit": "export"
}
```

</details>
