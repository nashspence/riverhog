# stove0_media_archive_target_contracts.MediaFieldPreference.unique_fields

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-contracts:stove0-media-archive-target-contracts-med-bc4591d41d:b7ea01a3ed -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-737d4ba3ce"></a>
- <a id="s-042ad36dde"></a>`distribution`: `stove0-media-archive-target-contracts`
- <a id="s-ad70f9408e"></a>`module`: `stove0_media_archive_target_contracts`
- <a id="s-a7b4efe630"></a>`name`: `unique_fields`
- <a id="s-09946834ee"></a>`owner`: `stove0_media_archive_target_contracts.MediaFieldPreference`
- <a id="s-927de253a9"></a>`unit`: `member`

### Declared structure

- <a id="s-6278b3f8bd"></a>`kind`: `"classmethod"`
- <a id="s-2d6c32f2bc"></a>`signature`: `"\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""`

## Maintained corroboration

### Related interface records

- [MediaFieldPreference](stove0-media-archive-target-contracts-mediafieldpreference.md)

## Governing policies

- <a id="pa-82a99c3e49"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-media-archive-target-contracts:stove0_media_archive_target_contracts](../../../evidence/sources/authorities.md#src-dfeb5229f2) — [some-implementations/stove0/targets/media-archive/contracts/src/stove0\_media\_archive\_target\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/contracts/src/stove0_media_archive_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_media_archive_target_contracts.MediaFieldPreference.unique_fields`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: eace29689003f7ac64b6ed687aff5ea30a8eb82f7b3b663ceb70a4ca5ac2a117 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""
  },
  "distribution": "stove0-media-archive-target-contracts",
  "module": "stove0_media_archive_target_contracts",
  "name": "unique_fields",
  "owner": "stove0_media_archive_target_contracts.MediaFieldPreference",
  "unit": "member"
}
```

</details>
