# stove0_media_archive_target_contracts.MediaProjectionPolicy.canonical_tags

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-contracts:stove0-media-archive-target-contracts-med-e42d70e1aa:b96fa0002a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-eb5f71e154"></a>
- <a id="s-1e4e953068"></a>`distribution`: `stove0-media-archive-target-contracts`
- <a id="s-50f9379838"></a>`module`: `stove0_media_archive_target_contracts`
- <a id="s-6c06dc4188"></a>`name`: `canonical_tags`
- <a id="s-fb18c64f00"></a>`owner`: `stove0_media_archive_target_contracts.MediaProjectionPolicy`
- <a id="s-aeb76e8dfd"></a>`unit`: `member`

### Declared structure

- <a id="s-14214f3531"></a>`kind`: `"classmethod"`
- <a id="s-e3cac37b1a"></a>`signature`: `"\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""`

## Maintained corroboration

### Related interface records

- [MediaProjectionPolicy](stove0-media-archive-target-contracts-mediaprojectionpolicy.md)

## Governing policies

- <a id="pa-f06c90d5ac"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-media-archive-target-contracts:stove0_media_archive_target_contracts](../../../evidence/sources/authorities.md#src-dfeb5229f2) — [some-implementations/stove0/targets/media-archive/contracts/src/stove0\_media\_archive\_target\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/contracts/src/stove0_media_archive_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_media_archive_target_contracts.MediaProjectionPolicy.canonical_tags`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7454c087f0575e212b65a18c3f78e660ea0619100f9b86a48202ebc37ac7f9e2 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""
  },
  "distribution": "stove0-media-archive-target-contracts",
  "module": "stove0_media_archive_target_contracts",
  "name": "canonical_tags",
  "owner": "stove0_media_archive_target_contracts.MediaProjectionPolicy",
  "unit": "member"
}
```

</details>
