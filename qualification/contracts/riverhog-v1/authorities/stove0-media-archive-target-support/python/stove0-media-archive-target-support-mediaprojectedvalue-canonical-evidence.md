# stove0_media_archive_target_support.MediaProjectedValue.canonical_evidence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-support:stove0-media-archive-target-support-media-de14303306:404c7596c0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-55ded5c62c"></a>
- <a id="s-96854716bb"></a>`distribution`: `stove0-media-archive-target-support`
- <a id="s-ba3c1bbf1c"></a>`module`: `stove0_media_archive_target_support`
- <a id="s-d123796c5c"></a>`name`: `canonical_evidence`
- <a id="s-efe1cfd679"></a>`owner`: `stove0_media_archive_target_support.MediaProjectedValue`
- <a id="s-8340b32e3d"></a>`unit`: `member`

### Declared structure

- <a id="s-8d1e8bfc87"></a>`kind`: `"classmethod"`
- <a id="s-7153dd9ff3"></a>`signature`: `"\"(cls, value: 'tuple[MediaFactEvidence, ...]') -> 'tuple[MediaFactEvidence, ...]'\""`

## Maintained corroboration

### Related interface records

- [MediaProjectedValue](stove0-media-archive-target-support-mediaprojectedvalue.md)

## Governing policies

- <a id="pa-3152eccae3"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-media-archive-target-support:stove0_media_archive_target_support](../../../evidence/sources/authorities.md#src-3caa343b1d) — [some-implementations/stove0/targets/media-archive/support/src/stove0\_media\_archive\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/support/src/stove0_media_archive_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_media_archive_target_support.MediaProjectedValue.canonical_evidence`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 284a6ac5ff35e1c059332bab72f80013bcf3da9e91c629295477d534efea2e82 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[MediaFactEvidence, ...]') -> 'tuple[MediaFactEvidence, ...]'\""
  },
  "distribution": "stove0-media-archive-target-support",
  "module": "stove0_media_archive_target_support",
  "name": "canonical_evidence",
  "owner": "stove0_media_archive_target_support.MediaProjectedValue",
  "unit": "member"
}
```

</details>
