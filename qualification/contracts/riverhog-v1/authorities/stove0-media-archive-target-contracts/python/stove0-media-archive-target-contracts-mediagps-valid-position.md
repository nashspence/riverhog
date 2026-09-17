# stove0_media_archive_target_contracts.MediaGps.valid_position

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-contracts:stove0-media-archive-target-contracts-med-b7c1f18093:069dafa5ee -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fd463946d6"></a>
- <a id="s-f16e016589"></a>`distribution`: `stove0-media-archive-target-contracts`
- <a id="s-c3803fccaa"></a>`module`: `stove0_media_archive_target_contracts`
- <a id="s-1b568acdfa"></a>`name`: `valid_position`
- <a id="s-96e83ae1ed"></a>`owner`: `stove0_media_archive_target_contracts.MediaGps`
- <a id="s-b36ec31ada"></a>`unit`: `member`

### Declared structure

- <a id="s-9707185f17"></a>`kind`: `"method"`
- <a id="s-a8007c2fe8"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [MediaGps](stove0-media-archive-target-contracts-mediagps.md)

## Governing policies

- <a id="pa-375929fc36"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-media-archive-target-contracts:stove0_media_archive_target_contracts](../../../evidence/sources/authorities.md#src-dfeb5229f2) — [reference/stove0/targets/media-archive/contracts/src/stove0\_media\_archive\_target\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/targets/media-archive/contracts/src/stove0_media_archive_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_media_archive_target_contracts.MediaGps.valid_position`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 266d5bd2cc5d9b8f5616c25e4f46544791a425fdba749e3615f2348940e2f3bb -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-media-archive-target-contracts",
  "module": "stove0_media_archive_target_contracts",
  "name": "valid_position",
  "owner": "stove0_media_archive_target_contracts.MediaGps",
  "unit": "member"
}
```

</details>
