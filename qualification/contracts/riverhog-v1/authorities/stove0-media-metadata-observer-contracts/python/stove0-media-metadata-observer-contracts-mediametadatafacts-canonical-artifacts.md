# stove0_media_metadata_observer_contracts.MediaMetadataFacts.canonical_artifacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-metadata-observer-contracts:stove0-media-metadata-observer-contracts-c190e97333:ffc5ecac8b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-metadata-observer-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b5d6964e16"></a>
| Field | Shape |
|---|---|
| <a id="s-11340ce524"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-77b78e3dab"></a>`distribution` | "stove0-media-metadata-observer-contracts" |
| <a id="s-3cc2bf8396"></a>`module` | "stove0_media_metadata_observer_contracts" |
| <a id="s-a42f0b3aab"></a>`name` | "canonical_artifacts" |
| <a id="s-ab733fe7e2"></a>`owner` | "stove0_media_metadata_observer_contracts.MediaMetadataFacts" |
| <a id="s-225f08c5a4"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_media_metadata_observer_contracts.MediaMetadataFacts](stove0-media-metadata-observer-contracts-mediametadatafacts.md)

## Governing policies

- <a id="pa-895d38cccf"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-metadata-observer-contracts:stove0_media_metadata_observer_contracts](../../../evidence/sources.md#src-8d1649a0f1) — `reference/stove0/observers/contracts/media-metadata/src/stove0_media_metadata_observer_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_metadata_observer_contracts.MediaMetadataFacts.canonical_artifacts`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f115931116fa08383ca9768ce856f729e0e62a5d0dee54b82a0f4d1ab4ddb503 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[MediaArtifactFacts, ...]') -> 'tuple[MediaArtifactFacts, ...]'\""
  },
  "distribution": "stove0-media-metadata-observer-contracts",
  "module": "stove0_media_metadata_observer_contracts",
  "name": "canonical_artifacts",
  "owner": "stove0_media_metadata_observer_contracts.MediaMetadataFacts",
  "unit": "member"
}
```
