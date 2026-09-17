# stove0_media_archive_target_support.MediaProjectionItem.canonical_path

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-support:stove0-media-archive-target-support-media-dcafba545e:1d318276d8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-31eff72afb"></a>
- <a id="s-f6bb8ca6f9"></a>`distribution`: `stove0-media-archive-target-support`
- <a id="s-b5d9153756"></a>`module`: `stove0_media_archive_target_support`
- <a id="s-f3277ea63a"></a>`name`: `canonical_path`
- <a id="s-a6b0a59ebb"></a>`owner`: `stove0_media_archive_target_support.MediaProjectionItem`
- <a id="s-25614b68c5"></a>`unit`: `member`

### Declared structure

- <a id="s-82fa7e01c6"></a>`kind`: `"classmethod"`
- <a id="s-9a0eafb950"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [MediaProjectionItem](stove0-media-archive-target-support-mediaprojectionitem.md)

## Governing policies

- <a id="pa-c69bee97ec"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-media-archive-target-support:stove0_media_archive_target_support](../../../evidence/sources.md#src-3caa343b1d) — [reference/stove0/targets/media-archive/support/src/stove0\_media\_archive\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/targets/media-archive/support/src/stove0_media_archive_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_media_archive_target_support.MediaProjectionItem.canonical_path`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0aac2b8aba5c18048d551ba9d2f1ca2375c542a99b66bdba01c9e60afe314dea -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "stove0-media-archive-target-support",
  "module": "stove0_media_archive_target_support",
  "name": "canonical_path",
  "owner": "stove0_media_archive_target_support.MediaProjectionItem",
  "unit": "member"
}
```

</details>
