# a_stove0_media_archive_contract_lib.AV1_OPUS_ARCHIVE_OPERATION_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-contract-lib:a-stove0-media-archive-contract-lib-av1-o-0d4fbf3c92:bd7010fb7f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-aa83285fe2"></a>
- <a id="s-21a96cc838"></a>`distribution`: `a-stove0-media-archive-contract-lib`
- <a id="s-b42452ab1f"></a>`module`: `a_stove0_media_archive_contract_lib`
- <a id="s-0dde73b69c"></a>`name`: `AV1_OPUS_ARCHIVE_OPERATION_ID`
- <a id="s-de61e0d231"></a>`unit`: `export`

### Declared structure

- <a id="s-578b906ee2"></a>`kind`: `"constant"`
- <a id="s-8a2289c590"></a>`value`: `"stove0.media.av1-opus-archive/v1"`

## Governing policies

- <a id="pa-9873bc9f85"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-contract-lib:a_stove0_media_archive_contract_lib](../../../evidence/sources/authorities.md#src-3f8c4376e0) — [some-implementations/stove0/targets/media-archive/contracts/src/a\_stove0\_media\_archive\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/contracts/src/a_stove0_media_archive_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_contract_lib.AV1_OPUS_ARCHIVE_OPERATION_ID`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bc7a7feb13d1de4e52a9d81548b6233709c67e2ad0193724501a7edc3be969d8 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0.media.av1-opus-archive/v1"
  },
  "distribution": "a-stove0-media-archive-contract-lib",
  "module": "a_stove0_media_archive_contract_lib",
  "name": "AV1_OPUS_ARCHIVE_OPERATION_ID",
  "unit": "export"
}
```

</details>
