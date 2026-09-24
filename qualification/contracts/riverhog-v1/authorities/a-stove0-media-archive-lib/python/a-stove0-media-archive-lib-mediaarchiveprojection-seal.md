# a_stove0_media_archive_lib.MediaArchiveProjection.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-lib:a-stove0-media-archive-lib-mediaarchivepr-d7332fbea9:79014c9595 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b4ee5e909e"></a>
- <a id="s-df9309ab76"></a>`distribution`: `a-stove0-media-archive-lib`
- <a id="s-4b7a893cc8"></a>`module`: `a_stove0_media_archive_lib`
- <a id="s-76dacd82dc"></a>`name`: `seal`
- <a id="s-e95b2d7dd6"></a>`owner`: `a_stove0_media_archive_lib.MediaArchiveProjection`
- <a id="s-fa438261bc"></a>`unit`: `member`

### Declared structure

- <a id="s-5e9d93feb6"></a>`kind`: `"classmethod"`
- <a id="s-89adf59666"></a>`signature`: `"\"(cls, payload: 'MediaArchiveProjectionPayload') -> 'MediaArchiveProjection'\""`

## Maintained corroboration

### Related interface records

- [MediaArchiveProjection](a-stove0-media-archive-lib-mediaarchiveprojection.md)

## Governing policies

- <a id="pa-dd925883e7"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-lib:a_stove0_media_archive_lib](../../../evidence/sources/authorities.md#src-6929294187) — [some-implementations/stove0/targets/media-archive/support/src/a\_stove0\_media\_archive\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/support/src/a_stove0_media_archive_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_lib.MediaArchiveProjection.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0c7c8138822591ef1a0a96890ef4cbc85c3292d24cbd08ee79918e4373760033 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'MediaArchiveProjectionPayload') -> 'MediaArchiveProjection'\""
  },
  "distribution": "a-stove0-media-archive-lib",
  "module": "a_stove0_media_archive_lib",
  "name": "seal",
  "owner": "a_stove0_media_archive_lib.MediaArchiveProjection",
  "unit": "member"
}
```

</details>
