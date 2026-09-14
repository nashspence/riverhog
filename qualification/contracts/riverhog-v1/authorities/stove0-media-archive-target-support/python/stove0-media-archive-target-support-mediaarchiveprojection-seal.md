# stove0_media_archive_target_support.MediaArchiveProjection.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-support:stove0-media-archive-target-support-media-aec1ebd382:589c68f76c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7ea3cc54ec"></a>
- <a id="s-8bb770c7b4"></a>`distribution`: `stove0-media-archive-target-support`
- <a id="s-a6f6bda93e"></a>`module`: `stove0_media_archive_target_support`
- <a id="s-188df304dd"></a>`name`: `seal`
- <a id="s-2631f438b9"></a>`owner`: `stove0_media_archive_target_support.MediaArchiveProjection`
- <a id="s-97bfca854e"></a>`unit`: `member`

### Declared structure

- <a id="s-6d721c315a"></a>`kind`: `"classmethod"`
- <a id="s-f1e67f956e"></a>`signature`: `"\"(cls, payload: 'MediaArchiveProjectionPayload') -> 'MediaArchiveProjection'\""`

## Maintained corroboration

### Related interface records

- [stove0_media_archive_target_support.MediaArchiveProjection](stove0-media-archive-target-support-mediaarchiveprojection.md)

## Governing policies

- <a id="pa-4e886b4973"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-support:stove0_media_archive_target_support](../../../evidence/sources.md#src-3caa343b1d) — `reference/stove0/targets/media-archive/support/src/stove0_media_archive_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_archive_target_support.MediaArchiveProjection.seal`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dd72c253ae5e8cd6a05caa3848507073c1c2395afdf7bc0eb1f4afd3a4fc8024 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'MediaArchiveProjectionPayload') -> 'MediaArchiveProjection'\""
  },
  "distribution": "stove0-media-archive-target-support",
  "module": "stove0_media_archive_target_support",
  "name": "seal",
  "owner": "stove0_media_archive_target_support.MediaArchiveProjection",
  "unit": "member"
}
```
