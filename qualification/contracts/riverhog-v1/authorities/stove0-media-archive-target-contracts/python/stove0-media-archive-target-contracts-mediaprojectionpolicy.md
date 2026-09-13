# stove0_media_archive_target_contracts.MediaProjectionPolicy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-contracts:stove0-media-archive-target-contracts-med-6d23f20f43:b93167371c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0583469c59"></a>
| Field | Shape |
|---|---|
| <a id="s-ead53debb9"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-e924690e61"></a>`distribution` | "stove0-media-archive-target-contracts" |
| <a id="s-786c5fda7e"></a>`module` | "stove0_media_archive_target_contracts" |
| <a id="s-08d103b95c"></a>`name` | "MediaProjectionPolicy" |
| <a id="s-b5c0bc4f38"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_media_archive_target_contracts.MediaProjectionPolicy.canonical_preferences](stove0-media-archive-target-contracts-mediaprojectionpolicy-canonical-preferences.md)
- [stove0_media_archive_target_contracts.MediaProjectionPolicy.canonical_creators](stove0-media-archive-target-contracts-mediaprojectionpolicy-canonical-creators.md)
- [stove0_media_archive_target_contracts.MediaProjectionPolicy.canonical_tags](stove0-media-archive-target-contracts-mediaprojectionpolicy-canonical-tags.md)
- [stove0_media_archive_target_contracts.MediaProjectionPolicy.canonical_optional_text](stove0-media-archive-target-contracts-mediaprojectionpolicy-canonical-optional-text.md)

## Governing policies

- <a id="pa-f31c9b1a62"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-contracts:stove0_media_archive_target_contracts](../../../evidence/sources.md#src-dfeb5229f2) — `reference/stove0/targets/media-archive/contracts/src/stove0_media_archive_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_archive_target_contracts.MediaProjectionPolicy`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3927e5d1a07fac8b88f9eee597fadb5f94bdaaefeb22a0d7328b22234ed7c664 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "77a33bbdb2a9e5f2d6191fab61273bc8f1eb5d357196a75ecf82c730a1dfd621",
    "signature": "\"(*, format: Literal['stove0-media-projection-policy/v1'] = 'stove0-media-projection-policy/v1', device_make: str | None = None, device_model: str | None = None, gps: stove0_media_archive_target_contracts.projection_policy.MediaGps | None = None, creators: tuple[str, ...] = (), tags: tuple[str, ...] = (), field_preferences: tuple[stove0_media_archive_target_contracts.projection_policy.MediaFieldPreference, ...] = ()) -> None\""
  },
  "distribution": "stove0-media-archive-target-contracts",
  "module": "stove0_media_archive_target_contracts",
  "name": "MediaProjectionPolicy",
  "unit": "export"
}
```
