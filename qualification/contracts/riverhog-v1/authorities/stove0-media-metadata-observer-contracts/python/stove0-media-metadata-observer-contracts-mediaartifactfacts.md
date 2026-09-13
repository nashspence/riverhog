# stove0_media_metadata_observer_contracts.MediaArtifactFacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-metadata-observer-contracts:stove0-media-metadata-observer-contracts-491157120e:25442dc6f9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-metadata-observer-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-21fca26a4b"></a>
| Field | Shape |
|---|---|
| <a id="s-2a1c616c74"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-1c307a5c8f"></a>`distribution` | "stove0-media-metadata-observer-contracts" |
| <a id="s-e3746e4d8f"></a>`module` | "stove0_media_metadata_observer_contracts" |
| <a id="s-34bc7b1003"></a>`name` | "MediaArtifactFacts" |
| <a id="s-d6100014d2"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_media_metadata_observer_contracts.MediaArtifactFacts.valid_state](stove0-media-metadata-observer-contracts-mediaartifactfacts-valid-state.md)

## Governing policies

- <a id="pa-c4153c653a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-metadata-observer-contracts:stove0_media_metadata_observer_contracts](../../../evidence/sources.md#src-8d1649a0f1) — `reference/stove0/observers/contracts/media-metadata/src/stove0_media_metadata_observer_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_metadata_observer_contracts.MediaArtifactFacts`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5648d7905fcc8516c1eae18069b31caddab51f8342530510a44051e622150157 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "64a2c3c20961d6282bdf02f4727399391df9329ca3212b0457d35d6f5dd60ce2",
    "signature": "\"(*, artifact_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], state: Literal['observed', 'unsupported'], facts: tuple[stove0_media_metadata_observer_contracts.contracts.MediaMetadataFact, ...] = ()) -> None\""
  },
  "distribution": "stove0-media-metadata-observer-contracts",
  "module": "stove0_media_metadata_observer_contracts",
  "name": "MediaArtifactFacts",
  "unit": "export"
}
```
