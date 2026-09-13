# stove0_media_archive_target_contracts.AudioArchiveIntent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-contracts:stove0-media-archive-target-contracts-aud-24045fc431:f2041a5c58 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e9beca85e7"></a>
| Field | Shape |
|---|---|
| <a id="s-88b5316569"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-68944ba22b"></a>`distribution` | "stove0-media-archive-target-contracts" |
| <a id="s-b998bed64a"></a>`module` | "stove0_media_archive_target_contracts" |
| <a id="s-d7a71a48ad"></a>`name` | "AudioArchiveIntent" |
| <a id="s-ad85ad5df2"></a>`unit` | "export" |

## Governing policies

- <a id="pa-fcbc8d0ea3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-contracts:stove0_media_archive_target_contracts](../../../evidence/sources.md#src-dfeb5229f2) — `reference/stove0/targets/media-archive/contracts/src/stove0_media_archive_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_archive_target_contracts.AudioArchiveIntent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 75851b38204f57103e1a8f87c4d7343cac5b343549b0c286c83c6ba5dcc189c0 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "ca8f0aa963958b27701a2b5a59429d23e242d2315c6d5361a9177c5a69d52456",
    "signature": "\"(*, codec: Literal['opus'] = 'opus', container: Literal['opus'] = 'opus', bitrate_kbps: Annotated[int, Ge(ge=16), Le(le=512)] = 128, metadata_projection: stove0_media_archive_target_contracts.projection_policy.MediaProjectionPolicy = <factory>) -> None\""
  },
  "distribution": "stove0-media-archive-target-contracts",
  "module": "stove0_media_archive_target_contracts",
  "name": "AudioArchiveIntent",
  "unit": "export"
}
```
