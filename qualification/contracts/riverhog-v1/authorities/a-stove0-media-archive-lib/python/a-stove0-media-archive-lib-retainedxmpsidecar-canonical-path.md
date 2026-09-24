# a_stove0_media_archive_lib.RetainedXmpSidecar.canonical_path

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-lib:a-stove0-media-archive-lib-retainedxmpsid-b64482e0f9:77777e1859 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e49777c61b"></a>
- <a id="s-314ae97188"></a>`distribution`: `a-stove0-media-archive-lib`
- <a id="s-d7bd863b60"></a>`module`: `a_stove0_media_archive_lib`
- <a id="s-641f84bdb0"></a>`name`: `canonical_path`
- <a id="s-c63658d5fb"></a>`owner`: `a_stove0_media_archive_lib.RetainedXmpSidecar`
- <a id="s-4353becf62"></a>`unit`: `member`

### Declared structure

- <a id="s-fcd6f9007b"></a>`kind`: `"classmethod"`
- <a id="s-61e81568a4"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [RetainedXmpSidecar](a-stove0-media-archive-lib-retainedxmpsidecar.md)

## Governing policies

- <a id="pa-26340058b9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-lib:a_stove0_media_archive_lib](../../../evidence/sources/authorities.md#src-6929294187) — [some-implementations/stove0/targets/media-archive/support/src/a\_stove0\_media\_archive\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/support/src/a_stove0_media_archive_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_lib.RetainedXmpSidecar.canonical_path`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8003f200489c31db6e60e35ef1a372efd19af63ef408ef873baea6e7a14eb1eb -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "a-stove0-media-archive-lib",
  "module": "a_stove0_media_archive_lib",
  "name": "canonical_path",
  "owner": "a_stove0_media_archive_lib.RetainedXmpSidecar",
  "unit": "member"
}
```

</details>
