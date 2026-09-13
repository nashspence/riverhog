# stove0_media_archive_target_contracts.MediaProjectionPolicy.canonical_preferences

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-contracts:stove0-media-archive-target-contracts-med-2b6b2ce61b:36c3c3f59e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ce5a74459d"></a>
| Field | Shape |
|---|---|
| <a id="s-abdfbcfe89"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-3a2ffde3e1"></a>`distribution` | "stove0-media-archive-target-contracts" |
| <a id="s-f422c3aff3"></a>`module` | "stove0_media_archive_target_contracts" |
| <a id="s-4d2c6f17d6"></a>`name` | "canonical_preferences" |
| <a id="s-dd0724d3ec"></a>`owner` | "stove0_media_archive_target_contracts.MediaProjectionPolicy" |
| <a id="s-63b4a9eb9d"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_media_archive_target_contracts.MediaProjectionPolicy](stove0-media-archive-target-contracts-mediaprojectionpolicy.md)

## Governing policies

- <a id="pa-700281eceb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-contracts:stove0_media_archive_target_contracts](../../../evidence/sources.md#src-dfeb5229f2) — `reference/stove0/targets/media-archive/contracts/src/stove0_media_archive_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_archive_target_contracts.MediaProjectionPolicy.canonical_preferences`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e10f1524b665b2f9bc4d7982ee0b00a59da5d372fd6ef6f4fe4a78931902e28a -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[MediaFieldPreference, ...]') -> 'tuple[MediaFieldPreference, ...]'\""
  },
  "distribution": "stove0-media-archive-target-contracts",
  "module": "stove0_media_archive_target_contracts",
  "name": "canonical_preferences",
  "owner": "stove0_media_archive_target_contracts.MediaProjectionPolicy",
  "unit": "member"
}
```
