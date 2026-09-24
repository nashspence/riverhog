# a_stove0_media_archive_lib.MediaProjectionItem.derived_from

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-lib:a-stove0-media-archive-lib-mediaprojectio-edcf92edcd:c499abb57d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1f47b99e5d"></a>
- <a id="s-0b3d14efe2"></a>`distribution`: `a-stove0-media-archive-lib`
- <a id="s-896595a4ac"></a>`module`: `a_stove0_media_archive_lib`
- <a id="s-89c3288875"></a>`name`: `derived_from`
- <a id="s-65b537ee62"></a>`owner`: `a_stove0_media_archive_lib.MediaProjectionItem`
- <a id="s-efe881525b"></a>`unit`: `member`

### Declared structure

- <a id="s-6bddf9cdc8"></a>`kind`: `"property"`
- <a id="s-97077002b2"></a>`signature`: `"\"(self) -> 'tuple[str, ...]'\""`

## Maintained corroboration

### Related interface records

- [MediaProjectionItem](a-stove0-media-archive-lib-mediaprojectionitem.md)

## Governing policies

- <a id="pa-e79dda16f7"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-lib:a_stove0_media_archive_lib](../../../evidence/sources/authorities.md#src-6929294187) — [some-implementations/stove0/targets/media-archive/support/src/a\_stove0\_media\_archive\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/support/src/a_stove0_media_archive_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_lib.MediaProjectionItem.derived_from`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0ca0fc0fef627902c70e7b6b23b95f72acbcb4b238f5c2b1c9883aec2bb92808 -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'tuple[str, ...]'\""
  },
  "distribution": "a-stove0-media-archive-lib",
  "module": "a_stove0_media_archive_lib",
  "name": "derived_from",
  "owner": "a_stove0_media_archive_lib.MediaProjectionItem",
  "unit": "member"
}
```

</details>
