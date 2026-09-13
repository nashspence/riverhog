# stove0_media_archive_target_support.MediaProjectedValue

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-support:stove0-media-archive-target-support-media-0d069711dc:edeaa692a2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dd46f38ca4"></a>
| Field | Shape |
|---|---|
| <a id="s-9a6530e2ed"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-32bfb70aa4"></a>`distribution` | "stove0-media-archive-target-support" |
| <a id="s-d5690a54a4"></a>`module` | "stove0_media_archive_target_support" |
| <a id="s-d475a78205"></a>`name` | "MediaProjectedValue" |
| <a id="s-e27301f78e"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_media_archive_target_support.MediaProjectedValue.canonical_evidence](stove0-media-archive-target-support-mediaprojectedvalue-canonical-evidence.md)
- [stove0_media_archive_target_support.MediaProjectedValue.bind_source](stove0-media-archive-target-support-mediaprojectedvalue-bind-source.md)

## Governing policies

- <a id="pa-a2fad9707f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-support:stove0_media_archive_target_support](../../../evidence/sources.md#src-3caa343b1d) — `reference/stove0/targets/media-archive/support/src/stove0_media_archive_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_archive_target_support.MediaProjectedValue`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 74c9865e9036492a0caaac82ffc566227113bf37341214a4efa9b55f01bb8144 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "cb40d7111242d2d59c8a440788114e49f192faf2baa7e1fdeca5876548348f44",
    "signature": "\"(*, name: Literal['capture-time', 'creator', 'device-make', 'device-model', 'gps-latitude', 'gps-longitude'], value: JsonValue, source: Literal['observation', 'recipe'], evidence: tuple[stove0_media_metadata_observer_contracts.contracts.MediaFactEvidence, ...] = ()) -> None\""
  },
  "distribution": "stove0-media-archive-target-support",
  "module": "stove0_media_archive_target_support",
  "name": "MediaProjectedValue",
  "unit": "export"
}
```
