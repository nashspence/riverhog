# a_stove0_media_archive_contract_lib.MediaFieldPreference.unique_fields

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-contract-lib:a-stove0-media-archive-contract-lib-media-29882f77b7:118f127ff5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c4e248fcd7"></a>
- <a id="s-bb616dd5ea"></a>`distribution`: `a-stove0-media-archive-contract-lib`
- <a id="s-2098e66166"></a>`module`: `a_stove0_media_archive_contract_lib`
- <a id="s-8d871be46b"></a>`name`: `unique_fields`
- <a id="s-d609a47acf"></a>`owner`: `a_stove0_media_archive_contract_lib.MediaFieldPreference`
- <a id="s-0f256372b6"></a>`unit`: `member`

### Declared structure

- <a id="s-11730e4e9f"></a>`kind`: `"classmethod"`
- <a id="s-15805f2f3e"></a>`signature`: `"\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""`

## Maintained corroboration

### Related interface records

- [MediaFieldPreference](a-stove0-media-archive-contract-lib-mediafieldpreference.md)

## Governing policies

- <a id="pa-1a570bc6ce"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-contract-lib:a_stove0_media_archive_contract_lib](../../../evidence/sources/authorities.md#src-3f8c4376e0) — [some-implementations/stove0/targets/media-archive/contracts/src/a\_stove0\_media\_archive\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/contracts/src/a_stove0_media_archive_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_contract_lib.MediaFieldPreference.unique_fields`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fa839681957822eb3d616a3e814c5a585acdd8b603ed9e08a62ad916ca6a5958 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""
  },
  "distribution": "a-stove0-media-archive-contract-lib",
  "module": "a_stove0_media_archive_contract_lib",
  "name": "unique_fields",
  "owner": "a_stove0_media_archive_contract_lib.MediaFieldPreference",
  "unit": "member"
}
```

</details>
