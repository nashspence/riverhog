# stove0_media_archive_target_contracts.Av1OpusArchiveIntent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-contracts:stove0-media-archive-target-contracts-av1-41e6936986:11eb09eaff -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5f18e790e7"></a>
| Field | Shape |
|---|---|
| <a id="s-542842c3fa"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-fb119dc711"></a>`distribution` | "stove0-media-archive-target-contracts" |
| <a id="s-fb1bdaf9bf"></a>`module` | "stove0_media_archive_target_contracts" |
| <a id="s-a2a7fab6c6"></a>`name` | "Av1OpusArchiveIntent" |
| <a id="s-42fb7b448c"></a>`unit` | "export" |

## Governing policies

- <a id="pa-934013709f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-contracts:stove0_media_archive_target_contracts](../../../evidence/sources.md#src-dfeb5229f2) — `reference/stove0/targets/media-archive/contracts/src/stove0_media_archive_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_archive_target_contracts.Av1OpusArchiveIntent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: da55139e6b5c4f95ba61b855bfd67513533f0a6fea8fd966d1de27d98e1da59e -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "3f8fff646a6b9081f6724972b146e71d8f20995d9a96c6da30e53b9bcb18094e",
    "signature": "\"(*, codec: Literal['av1'] = 'av1', container: Literal['mkv'] = 'mkv', quality: Annotated[int, Ge(ge=0), Le(le=63)] = 23, max_height: Annotated[int | None, Ge(ge=144), Le(le=8640)] = None, audio_bitrate_kbps: Annotated[int, Ge(ge=16), Le(le=512)] = 128, salvage: Literal['off', 'safe-remux'] = 'safe-remux', metadata_projection: stove0_media_archive_target_contracts.projection_policy.MediaProjectionPolicy = <factory>) -> None\""
  },
  "distribution": "stove0-media-archive-target-contracts",
  "module": "stove0_media_archive_target_contracts",
  "name": "Av1OpusArchiveIntent",
  "unit": "export"
}
```
