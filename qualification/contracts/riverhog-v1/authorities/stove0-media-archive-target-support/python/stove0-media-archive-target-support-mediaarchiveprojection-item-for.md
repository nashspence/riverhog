# stove0_media_archive_target_support.MediaArchiveProjection.item_for

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-support:stove0-media-archive-target-support-media-109c9bfc5d:7bd89f1f1c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bee7cfb201"></a>
- <a id="s-c14c6ffe1f"></a>`distribution`: `stove0-media-archive-target-support`
- <a id="s-394d8dc318"></a>`module`: `stove0_media_archive_target_support`
- <a id="s-3641682eb4"></a>`name`: `item_for`
- <a id="s-b0ad204cee"></a>`owner`: `stove0_media_archive_target_support.MediaArchiveProjection`
- <a id="s-0dae90c6fd"></a>`unit`: `member`

### Declared structure

- <a id="s-fa56d82264"></a>`kind`: `"method"`
- <a id="s-192ae2c0f3"></a>`signature`: `"\"(self, artifact_id: 'str') -> 'MediaProjectionItem'\""`

## Maintained corroboration

### Related interface records

- [MediaArchiveProjection](stove0-media-archive-target-support-mediaarchiveprojection.md)

## Governing policies

- <a id="pa-bd2d5dd345"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-media-archive-target-support:stove0_media_archive_target_support](../../../evidence/sources/authorities.md#src-3caa343b1d) — [reference/stove0/targets/media-archive/support/src/stove0\_media\_archive\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/targets/media-archive/support/src/stove0_media_archive_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_media_archive_target_support.MediaArchiveProjection.item_for`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e35018d9c2cacae32c3784294a3a57b482a53e39545f012a5e6c7a14ec842930 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, artifact_id: 'str') -> 'MediaProjectionItem'\""
  },
  "distribution": "stove0-media-archive-target-support",
  "module": "stove0_media_archive_target_support",
  "name": "item_for",
  "owner": "stove0_media_archive_target_support.MediaArchiveProjection",
  "unit": "member"
}
```

</details>
