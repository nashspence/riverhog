# a_stove0_media_archive_lib.MediaProjectionItem.canonical_path

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-lib:a-stove0-media-archive-lib-mediaprojectio-95ec401deb:23503b530f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cc76d4b42d"></a>
- <a id="s-51269bd43d"></a>`distribution`: `a-stove0-media-archive-lib`
- <a id="s-248cbf95ca"></a>`module`: `a_stove0_media_archive_lib`
- <a id="s-b1d5999c5a"></a>`name`: `canonical_path`
- <a id="s-061aacfcb0"></a>`owner`: `a_stove0_media_archive_lib.MediaProjectionItem`
- <a id="s-507cf9256b"></a>`unit`: `member`

### Declared structure

- <a id="s-63e76899dc"></a>`kind`: `"classmethod"`
- <a id="s-f76008dadc"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [MediaProjectionItem](a-stove0-media-archive-lib-mediaprojectionitem.md)

## Governing policies

- <a id="pa-82a3cbafb9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-lib:a_stove0_media_archive_lib](../../../evidence/sources/authorities.md#src-6929294187) — [some-implementations/stove0/targets/media-archive/support/src/a\_stove0\_media\_archive\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/support/src/a_stove0_media_archive_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_lib.MediaProjectionItem.canonical_path`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1c89a94d28a29eeb16fd80a905d03dbfe1057be389affd6c6da211ccd9f8c634 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "a-stove0-media-archive-lib",
  "module": "a_stove0_media_archive_lib",
  "name": "canonical_path",
  "owner": "a_stove0_media_archive_lib.MediaProjectionItem",
  "unit": "member"
}
```

</details>
