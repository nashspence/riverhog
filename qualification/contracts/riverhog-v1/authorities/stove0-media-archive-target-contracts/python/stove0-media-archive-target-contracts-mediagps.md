# stove0_media_archive_target_contracts.MediaGps

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-contracts:stove0-media-archive-target-contracts-mediagps:f0d20946c2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8e6bab8514"></a>
| Field | Shape |
|---|---|
| <a id="s-1eb5bfa593"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-216497b241"></a>`distribution` | "stove0-media-archive-target-contracts" |
| <a id="s-8ad65404e7"></a>`module` | "stove0_media_archive_target_contracts" |
| <a id="s-ffc271b039"></a>`name` | "MediaGps" |
| <a id="s-3d733cbd5d"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_media_archive_target_contracts.MediaGps.valid_position](stove0-media-archive-target-contracts-mediagps-valid-position.md)

## Governing policies

- <a id="pa-4308186fcd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-contracts:stove0_media_archive_target_contracts](../../../evidence/sources.md#src-dfeb5229f2) — `reference/stove0/targets/media-archive/contracts/src/stove0_media_archive_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_archive_target_contracts.MediaGps`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b7038a49c205f2c9e217a0d0d49fbbd0a04b61121b3f91de6642b8785ff6d063 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "034cbe1a80429c82cacda24116ace3547aae0d8cfe2dd1e814387d6ce49a8325",
    "signature": "'(*, latitude: float, longitude: float) -> None'"
  },
  "distribution": "stove0-media-archive-target-contracts",
  "module": "stove0_media_archive_target_contracts",
  "name": "MediaGps",
  "unit": "export"
}
```
