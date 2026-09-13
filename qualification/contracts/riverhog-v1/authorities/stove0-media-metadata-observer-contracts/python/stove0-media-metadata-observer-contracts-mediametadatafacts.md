# stove0_media_metadata_observer_contracts.MediaMetadataFacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-metadata-observer-contracts:stove0-media-metadata-observer-contracts-74ce689e0f:72db9f0f50 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-metadata-observer-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e06e788609"></a>
| Field | Shape |
|---|---|
| <a id="s-dba2156c3b"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-1e11dc0855"></a>`distribution` | "stove0-media-metadata-observer-contracts" |
| <a id="s-6d880e8fc3"></a>`module` | "stove0_media_metadata_observer_contracts" |
| <a id="s-51a11ed78f"></a>`name` | "MediaMetadataFacts" |
| <a id="s-3550931bce"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_media_metadata_observer_contracts.MediaMetadataFacts.canonical_artifacts](stove0-media-metadata-observer-contracts-mediametadatafacts-canonical-artifacts.md)

## Governing policies

- <a id="pa-fde4d796b2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-metadata-observer-contracts:stove0_media_metadata_observer_contracts](../../../evidence/sources.md#src-8d1649a0f1) — `reference/stove0/observers/contracts/media-metadata/src/stove0_media_metadata_observer_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_metadata_observer_contracts.MediaMetadataFacts`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3c6069e696bfbfe94bbfcc564b5ed500fa9a2d0940a174340989ecb273c203e5 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "624c9687b02b9d304bb6fb8f726e6de14b7a9dae1132e4f689254cdba020d73e",
    "signature": "'(*, artifacts: Annotated[tuple[stove0_media_metadata_observer_contracts.contracts.MediaArtifactFacts, ...], MinLen(min_length=1)]) -> None'"
  },
  "distribution": "stove0-media-metadata-observer-contracts",
  "module": "stove0_media_metadata_observer_contracts",
  "name": "MediaMetadataFacts",
  "unit": "export"
}
```
