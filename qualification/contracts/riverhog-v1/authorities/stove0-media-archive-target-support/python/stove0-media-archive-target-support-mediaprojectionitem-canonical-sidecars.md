# stove0_media_archive_target_support.MediaProjectionItem.canonical_sidecars

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-support:stove0-media-archive-target-support-media-fc2e122569:6502f70eca -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-976fc5dd91"></a>
- <a id="s-f763297755"></a>`distribution`: `stove0-media-archive-target-support`
- <a id="s-741d0377e1"></a>`module`: `stove0_media_archive_target_support`
- <a id="s-e4f0ef3d82"></a>`name`: `canonical_sidecars`
- <a id="s-01aed5e63b"></a>`owner`: `stove0_media_archive_target_support.MediaProjectionItem`
- <a id="s-eecbe0aa8b"></a>`unit`: `member`

### Declared structure

- <a id="s-a7b3b57e48"></a>`kind`: `"classmethod"`
- <a id="s-85ed09bd85"></a>`signature`: `"\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""`

## Maintained corroboration

### Related interface records

- [MediaProjectionItem](stove0-media-archive-target-support-mediaprojectionitem.md)

## Governing policies

- <a id="pa-36586ff6f3"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-media-archive-target-support:stove0_media_archive_target_support](../../../evidence/sources/authorities.md#src-3caa343b1d) — [reference/stove0/targets/media-archive/support/src/stove0\_media\_archive\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/targets/media-archive/support/src/stove0_media_archive_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_media_archive_target_support.MediaProjectionItem.canonical_sidecars`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8921d7f07c3889b907366c65cbebaf4188b02ee4a12edfe92eae6baaa0a5a314 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""
  },
  "distribution": "stove0-media-archive-target-support",
  "module": "stove0_media_archive_target_support",
  "name": "canonical_sidecars",
  "owner": "stove0_media_archive_target_support.MediaProjectionItem",
  "unit": "member"
}
```

</details>
