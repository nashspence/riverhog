# stove0_media_archive_target_support.ffmpeg_container_metadata_args

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-support:stove0-media-archive-target-support-ffmpe-0b218f02a8:a4bdaa4b48 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c0727bf86c"></a>
- <a id="s-446fe6bae3"></a>`distribution`: `stove0-media-archive-target-support`
- <a id="s-adc2e53801"></a>`module`: `stove0_media_archive_target_support`
- <a id="s-149ae26d17"></a>`name`: `ffmpeg_container_metadata_args`
- <a id="s-34e4fd39bc"></a>`unit`: `export`

### Declared structure

- <a id="s-b5c814e42c"></a>`kind`: `"function"`
- <a id="s-aa93b24710"></a>`signature`: `"\"(item: 'MediaProjectionItem') -> 'list[str]'\""`

## Governing policies

- <a id="pa-1c42ca514f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-support:stove0_media_archive_target_support](../../../evidence/sources.md#src-3caa343b1d) — `reference/stove0/targets/media-archive/support/src/stove0_media_archive_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_archive_target_support.ffmpeg_container_metadata_args`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a76b9db6f958de49826f242e96ae4740c572b9bc989f789cdca496c35a91027b -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(item: 'MediaProjectionItem') -> 'list[str]'\""
  },
  "distribution": "stove0-media-archive-target-support",
  "module": "stove0_media_archive_target_support",
  "name": "ffmpeg_container_metadata_args",
  "unit": "export"
}
```
