# a_stove0_media_archive_contract_lib.MediaProjectionPolicy.canonical_tags

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-contract-lib:a-stove0-media-archive-contract-lib-media-65077132da:9582141878 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1aa927af19"></a>
- <a id="s-7a9a09d0f7"></a>`distribution`: `a-stove0-media-archive-contract-lib`
- <a id="s-a62b1ccf30"></a>`module`: `a_stove0_media_archive_contract_lib`
- <a id="s-34d9bd3340"></a>`name`: `canonical_tags`
- <a id="s-8385a9bdb4"></a>`owner`: `a_stove0_media_archive_contract_lib.MediaProjectionPolicy`
- <a id="s-0a8dd0c6d8"></a>`unit`: `member`

### Declared structure

- <a id="s-a8742da849"></a>`kind`: `"classmethod"`
- <a id="s-468ed3779e"></a>`signature`: `"\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""`

## Maintained corroboration

### Related interface records

- [MediaProjectionPolicy](a-stove0-media-archive-contract-lib-mediaprojectionpolicy.md)

## Governing policies

- <a id="pa-ad4099c8b0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-contract-lib:a_stove0_media_archive_contract_lib](../../../evidence/sources/authorities.md#src-3f8c4376e0) — [some-implementations/stove0/targets/media-archive/contracts/src/a\_stove0\_media\_archive\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/contracts/src/a_stove0_media_archive_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_contract_lib.MediaProjectionPolicy.canonical_tags`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fc9cb02f757003fa578af73585b58b169cb1f54b1618b115c1d21a3fbe917540 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""
  },
  "distribution": "a-stove0-media-archive-contract-lib",
  "module": "a_stove0_media_archive_contract_lib",
  "name": "canonical_tags",
  "owner": "a_stove0_media_archive_contract_lib.MediaProjectionPolicy",
  "unit": "member"
}
```

</details>
