# stove0_media_metadata_observer_contracts.MediaMetadataFact

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-metadata-observer-contracts:stove0-media-metadata-observer-contracts-fb0f5ce0ed:3149c27bc6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-metadata-observer-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-99aa99dbbe"></a>
| Field | Shape |
|---|---|
| <a id="s-47d392e90d"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-9c778d8d73"></a>`distribution` | "stove0-media-metadata-observer-contracts" |
| <a id="s-4ac12ed9fa"></a>`module` | "stove0_media_metadata_observer_contracts" |
| <a id="s-e5a61a906d"></a>`name` | "MediaMetadataFact" |
| <a id="s-bdecce9aa4"></a>`unit` | "export" |

## Governing policies

- <a id="pa-16b59f8075"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-metadata-observer-contracts:stove0_media_metadata_observer_contracts](../../../evidence/sources.md#src-8d1649a0f1) — `reference/stove0/observers/contracts/media-metadata/src/stove0_media_metadata_observer_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_metadata_observer_contracts.MediaMetadataFact`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 310d5bf5d6c63faba7aaad2bba87a255b95cf03bbd0b9275f239fcdc64486b70 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "6bf6c513c1eaffa8790cfceb1ed51efcb1005ba23283931318c474e2d251aca0",
    "signature": "\"(*, name: Literal['capture-time', 'container-format', 'creator', 'device-make', 'device-model', 'gps-latitude', 'gps-longitude'], value: JsonValue, evidence: stove0_media_metadata_observer_contracts.contracts.MediaFactEvidence) -> None\""
  },
  "distribution": "stove0-media-metadata-observer-contracts",
  "module": "stove0_media_metadata_observer_contracts",
  "name": "MediaMetadataFact",
  "unit": "export"
}
```
