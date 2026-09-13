# stove0_media_archive_target_support.MediaProjectionItem

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-support:stove0-media-archive-target-support-media-7ca9389034:3cd6bb6a77 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b033958ec2"></a>
| Field | Shape |
|---|---|
| <a id="s-09e51b35b7"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-693f47030c"></a>`distribution` | "stove0-media-archive-target-support" |
| <a id="s-150d06f3f1"></a>`module` | "stove0_media_archive_target_support" |
| <a id="s-d7e02a93cd"></a>`name` | "MediaProjectionItem" |
| <a id="s-62122ca441"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_media_archive_target_support.MediaProjectionItem.canonical_evidence](stove0-media-archive-target-support-mediaprojectionitem-canonical-evidence.md)
- [stove0_media_archive_target_support.MediaProjectionItem.derived_from](stove0-media-archive-target-support-mediaprojectionitem-derived-from.md)
- [stove0_media_archive_target_support.MediaProjectionItem.canonical_path](stove0-media-archive-target-support-mediaprojectionitem-canonical-path.md)
- [stove0_media_archive_target_support.MediaProjectionItem.canonical_sidecars](stove0-media-archive-target-support-mediaprojectionitem-canonical-sidecars.md)

## Governing policies

- <a id="pa-d7cda3ce73"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-support:stove0_media_archive_target_support](../../../evidence/sources.md#src-3caa343b1d) — `reference/stove0/targets/media-archive/support/src/stove0_media_archive_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_archive_target_support.MediaProjectionItem`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 27e748c6aa1c8efdadcb9299f3c9ae69cfbc2aa4455a99414678d464e5427a2d -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "66e94ffdb8669f096b49716657bc94b8f025e37ebf2e15ac8fe28451448ede3c",
    "signature": "'(*, input_artifact_id: str, associated_sidecar_artifact_ids: tuple[str, ...] = (), archive_path: str, xmp_path: str, assertions: tuple[stove0_media_metadata_observer_contracts.contracts.MediaMetadataFact, ...] = (), selected: tuple[stove0_media_archive_target_support.projection.MediaProjectedValue, ...] = ()) -> None'"
  },
  "distribution": "stove0-media-archive-target-support",
  "module": "stove0_media_archive_target_support",
  "name": "MediaProjectionItem",
  "unit": "export"
}
```
