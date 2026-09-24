# a_stove0_media_archive_contract_lib.MediaProjectionPolicy.canonical_optional_text

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-contract-lib:a-stove0-media-archive-contract-lib-media-0a441c6093:c620685903 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5c5bdb1215"></a>
- <a id="s-9f90c8c744"></a>`distribution`: `a-stove0-media-archive-contract-lib`
- <a id="s-5bd1278f3b"></a>`module`: `a_stove0_media_archive_contract_lib`
- <a id="s-b9c789b363"></a>`name`: `canonical_optional_text`
- <a id="s-fd2f5cf0fc"></a>`owner`: `a_stove0_media_archive_contract_lib.MediaProjectionPolicy`
- <a id="s-0eb297b0b4"></a>`unit`: `member`

### Declared structure

- <a id="s-05eb5ef324"></a>`kind`: `"classmethod"`
- <a id="s-1f6c32f38c"></a>`signature`: `"\"(cls, value: 'str \| None') -> 'str \| None'\""`

## Maintained corroboration

### Related interface records

- [MediaProjectionPolicy](a-stove0-media-archive-contract-lib-mediaprojectionpolicy.md)

## Governing policies

- <a id="pa-00d7abd523"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-contract-lib:a_stove0_media_archive_contract_lib](../../../evidence/sources/authorities.md#src-3f8c4376e0) — [some-implementations/stove0/targets/media-archive/contracts/src/a\_stove0\_media\_archive\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/contracts/src/a_stove0_media_archive_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_contract_lib.MediaProjectionPolicy.canonical_optional_text`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 55d2be9ee5b3d2a7be37079aad2f076c049763dfdda3ece150401b59e4e0509b -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str | None') -> 'str | None'\""
  },
  "distribution": "a-stove0-media-archive-contract-lib",
  "module": "a_stove0_media_archive_contract_lib",
  "name": "canonical_optional_text",
  "owner": "a_stove0_media_archive_contract_lib.MediaProjectionPolicy",
  "unit": "member"
}
```

</details>
