# a_stove0_media_archive_lib.MediaProjectionItem.canonical_sidecars

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-lib:a-stove0-media-archive-lib-mediaprojectio-7e879fd2a5:34267f838e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3afe9f3310"></a>
- <a id="s-29ab53cc09"></a>`distribution`: `a-stove0-media-archive-lib`
- <a id="s-46e221367a"></a>`module`: `a_stove0_media_archive_lib`
- <a id="s-0432f1031a"></a>`name`: `canonical_sidecars`
- <a id="s-1d65de0ae0"></a>`owner`: `a_stove0_media_archive_lib.MediaProjectionItem`
- <a id="s-116312181d"></a>`unit`: `member`

### Declared structure

- <a id="s-0f13833dc4"></a>`kind`: `"classmethod"`
- <a id="s-79a00246c7"></a>`signature`: `"\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""`

## Maintained corroboration

### Related interface records

- [MediaProjectionItem](a-stove0-media-archive-lib-mediaprojectionitem.md)

## Governing policies

- <a id="pa-4b9c2fed2d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-lib:a_stove0_media_archive_lib](../../../evidence/sources/authorities.md#src-6929294187) — [some-implementations/stove0/targets/media-archive/support/src/a\_stove0\_media\_archive\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/support/src/a_stove0_media_archive_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_lib.MediaProjectionItem.canonical_sidecars`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b0f8c1b374c3d215b894604819e811ba0654ffc3583c95affed887e1275f22d8 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""
  },
  "distribution": "a-stove0-media-archive-lib",
  "module": "a_stove0_media_archive_lib",
  "name": "canonical_sidecars",
  "owner": "a_stove0_media_archive_lib.MediaProjectionItem",
  "unit": "member"
}
```

</details>
