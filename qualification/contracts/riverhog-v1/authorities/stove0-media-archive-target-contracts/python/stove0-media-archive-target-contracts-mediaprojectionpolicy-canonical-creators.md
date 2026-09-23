# stove0_media_archive_target_contracts.MediaProjectionPolicy.canonical_creators

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-contracts:stove0-media-archive-target-contracts-med-dc77eb069a:e9bce5e7b5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f3e9b383f1"></a>
- <a id="s-948fe25b24"></a>`distribution`: `stove0-media-archive-target-contracts`
- <a id="s-12e2c16407"></a>`module`: `stove0_media_archive_target_contracts`
- <a id="s-5be13dce7d"></a>`name`: `canonical_creators`
- <a id="s-5e11546b99"></a>`owner`: `stove0_media_archive_target_contracts.MediaProjectionPolicy`
- <a id="s-e2161643c3"></a>`unit`: `member`

### Declared structure

- <a id="s-c9338bbd59"></a>`kind`: `"classmethod"`
- <a id="s-14d6af0fe6"></a>`signature`: `"\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""`

## Maintained corroboration

### Related interface records

- [MediaProjectionPolicy](stove0-media-archive-target-contracts-mediaprojectionpolicy.md)

## Governing policies

- <a id="pa-6e78144437"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-media-archive-target-contracts:stove0_media_archive_target_contracts](../../../evidence/sources/authorities.md#src-dfeb5229f2) — [some-implementations/stove0/targets/media-archive/contracts/src/stove0\_media\_archive\_target\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/contracts/src/stove0_media_archive_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_media_archive_target_contracts.MediaProjectionPolicy.canonical_creators`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fcebbe81e67ac11a8debfd83acc6d69f1a84c379b347d20eba73f054a4f6fcbe -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""
  },
  "distribution": "stove0-media-archive-target-contracts",
  "module": "stove0_media_archive_target_contracts",
  "name": "canonical_creators",
  "owner": "stove0_media_archive_target_contracts.MediaProjectionPolicy",
  "unit": "member"
}
```

</details>
