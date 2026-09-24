# a_stove0_media_archive_lib.MediaArchiveProjection.item_for

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-lib:a-stove0-media-archive-lib-mediaarchivepr-f134a15f73:3c7ec657e0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-50e9789687"></a>
- <a id="s-a8c42a1286"></a>`distribution`: `a-stove0-media-archive-lib`
- <a id="s-689cf0ed32"></a>`module`: `a_stove0_media_archive_lib`
- <a id="s-0dea3ab7b4"></a>`name`: `item_for`
- <a id="s-935d9eac78"></a>`owner`: `a_stove0_media_archive_lib.MediaArchiveProjection`
- <a id="s-922db614da"></a>`unit`: `member`

### Declared structure

- <a id="s-57be542d21"></a>`kind`: `"method"`
- <a id="s-ea93ead975"></a>`signature`: `"\"(self, artifact_id: 'str') -> 'MediaProjectionItem'\""`

## Maintained corroboration

### Related interface records

- [MediaArchiveProjection](a-stove0-media-archive-lib-mediaarchiveprojection.md)

## Governing policies

- <a id="pa-a390202de7"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-lib:a_stove0_media_archive_lib](../../../evidence/sources/authorities.md#src-6929294187) — [some-implementations/stove0/targets/media-archive/support/src/a\_stove0\_media\_archive\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/support/src/a_stove0_media_archive_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_lib.MediaArchiveProjection.item_for`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6e63a86e7cc4eab47a0fc0f512580d516e656a94db71fedfe811239020e1d7ed -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, artifact_id: 'str') -> 'MediaProjectionItem'\""
  },
  "distribution": "a-stove0-media-archive-lib",
  "module": "a_stove0_media_archive_lib",
  "name": "item_for",
  "owner": "a_stove0_media_archive_lib.MediaArchiveProjection",
  "unit": "member"
}
```

</details>
