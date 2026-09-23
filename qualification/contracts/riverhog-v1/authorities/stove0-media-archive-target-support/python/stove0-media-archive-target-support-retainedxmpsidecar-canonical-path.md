# stove0_media_archive_target_support.RetainedXmpSidecar.canonical_path

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-support:stove0-media-archive-target-support-retai-7cef01cf5f:d9acbd86cc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dd5c2518f8"></a>
- <a id="s-e4e492c01f"></a>`distribution`: `stove0-media-archive-target-support`
- <a id="s-2e94e70f19"></a>`module`: `stove0_media_archive_target_support`
- <a id="s-29c0802a75"></a>`name`: `canonical_path`
- <a id="s-bb899fd07b"></a>`owner`: `stove0_media_archive_target_support.RetainedXmpSidecar`
- <a id="s-db9d8dc65f"></a>`unit`: `member`

### Declared structure

- <a id="s-18428d9f61"></a>`kind`: `"classmethod"`
- <a id="s-c06b73a353"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [RetainedXmpSidecar](stove0-media-archive-target-support-retainedxmpsidecar.md)

## Governing policies

- <a id="pa-885129a3ef"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-media-archive-target-support:stove0_media_archive_target_support](../../../evidence/sources/authorities.md#src-3caa343b1d) — [some-implementations/stove0/targets/media-archive/support/src/stove0\_media\_archive\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/support/src/stove0_media_archive_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_media_archive_target_support.RetainedXmpSidecar.canonical_path`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9bfe774ec43b989bb0299d483e17be3be53c715c701fee1d211175fc7842a08c -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "stove0-media-archive-target-support",
  "module": "stove0_media_archive_target_support",
  "name": "canonical_path",
  "owner": "stove0_media_archive_target_support.RetainedXmpSidecar",
  "unit": "member"
}
```

</details>
