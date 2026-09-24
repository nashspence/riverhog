# a_stove0_media_archive_lib.MediaProjectedValue.canonical_evidence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-lib:a-stove0-media-archive-lib-mediaprojected-7f872afa8d:5f47ecb270 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-61ab79807a"></a>
- <a id="s-f21be7d1ac"></a>`distribution`: `a-stove0-media-archive-lib`
- <a id="s-bf6773efa3"></a>`module`: `a_stove0_media_archive_lib`
- <a id="s-a22914747a"></a>`name`: `canonical_evidence`
- <a id="s-dc0c5d7099"></a>`owner`: `a_stove0_media_archive_lib.MediaProjectedValue`
- <a id="s-dbd15bac16"></a>`unit`: `member`

### Declared structure

- <a id="s-561b7b3ae9"></a>`kind`: `"classmethod"`
- <a id="s-0fe77384dd"></a>`signature`: `"\"(cls, value: 'tuple[MediaFactEvidence, ...]') -> 'tuple[MediaFactEvidence, ...]'\""`

## Maintained corroboration

### Related interface records

- [MediaProjectedValue](a-stove0-media-archive-lib-mediaprojectedvalue.md)

## Governing policies

- <a id="pa-4330810983"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-lib:a_stove0_media_archive_lib](../../../evidence/sources/authorities.md#src-6929294187) — [some-implementations/stove0/targets/media-archive/support/src/a\_stove0\_media\_archive\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/support/src/a_stove0_media_archive_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_lib.MediaProjectedValue.canonical_evidence`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ff84fe1262d3dd766be0ea1da266dd98126377608b5d7260ed6941f42df908f8 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[MediaFactEvidence, ...]') -> 'tuple[MediaFactEvidence, ...]'\""
  },
  "distribution": "a-stove0-media-archive-lib",
  "module": "a_stove0_media_archive_lib",
  "name": "canonical_evidence",
  "owner": "a_stove0_media_archive_lib.MediaProjectedValue",
  "unit": "member"
}
```

</details>
