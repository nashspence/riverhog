# stove0_media_archive_target_support.MediaProjectionItem.derived_from

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-support:stove0-media-archive-target-support-media-c56a12894c:78e94ed91b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e3d1e841b1"></a>
| Field | Shape |
|---|---|
| <a id="s-d78ab190ff"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-aee6dd0932"></a>`distribution` | "stove0-media-archive-target-support" |
| <a id="s-fa35fa4d0a"></a>`module` | "stove0_media_archive_target_support" |
| <a id="s-e7b5cb9560"></a>`name` | "derived_from" |
| <a id="s-8417a729e7"></a>`owner` | "stove0_media_archive_target_support.MediaProjectionItem" |
| <a id="s-1263ce2fc8"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_media_archive_target_support.MediaProjectionItem](stove0-media-archive-target-support-mediaprojectionitem.md)

## Governing policies

- <a id="pa-7601146f1a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-support:stove0_media_archive_target_support](../../../evidence/sources.md#src-3caa343b1d) — `reference/stove0/targets/media-archive/support/src/stove0_media_archive_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_archive_target_support.MediaProjectionItem.derived_from`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 569e01db5d048e67d03d5e022f8bea6da91b46d79755dab7d86cf3f58038538b -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'tuple[str, ...]'\""
  },
  "distribution": "stove0-media-archive-target-support",
  "module": "stove0_media_archive_target_support",
  "name": "derived_from",
  "owner": "stove0_media_archive_target_support.MediaProjectionItem",
  "unit": "member"
}
```
