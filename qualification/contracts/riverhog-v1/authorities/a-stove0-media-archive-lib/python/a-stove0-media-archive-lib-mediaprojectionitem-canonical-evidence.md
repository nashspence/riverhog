# a_stove0_media_archive_lib.MediaProjectionItem.canonical_evidence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-lib:a-stove0-media-archive-lib-mediaprojectio-acfe250ef2:24ca6b6552 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f39a97ce18"></a>
- <a id="s-676d65f08b"></a>`distribution`: `a-stove0-media-archive-lib`
- <a id="s-b07cddd507"></a>`module`: `a_stove0_media_archive_lib`
- <a id="s-2d2a5dbf65"></a>`name`: `canonical_evidence`
- <a id="s-fdf21c116d"></a>`owner`: `a_stove0_media_archive_lib.MediaProjectionItem`
- <a id="s-c15782d960"></a>`unit`: `member`

### Declared structure

- <a id="s-44525d06b9"></a>`kind`: `"method"`
- <a id="s-9a02c8c608"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [MediaProjectionItem](a-stove0-media-archive-lib-mediaprojectionitem.md)

## Governing policies

- <a id="pa-9846e7e864"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-lib:a_stove0_media_archive_lib](../../../evidence/sources/authorities.md#src-6929294187) — [some-implementations/stove0/targets/media-archive/support/src/a\_stove0\_media\_archive\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/support/src/a_stove0_media_archive_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_lib.MediaProjectionItem.canonical_evidence`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 440b3b3e52dfddd4ddc803c5ae4bb9bb745925322eca1bde8b2192bbd1fe1241 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "a-stove0-media-archive-lib",
  "module": "a_stove0_media_archive_lib",
  "name": "canonical_evidence",
  "owner": "a_stove0_media_archive_lib.MediaProjectionItem",
  "unit": "member"
}
```

</details>
