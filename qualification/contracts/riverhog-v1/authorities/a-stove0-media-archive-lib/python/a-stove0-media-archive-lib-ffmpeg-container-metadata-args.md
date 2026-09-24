# a_stove0_media_archive_lib.ffmpeg_container_metadata_args

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-lib:a-stove0-media-archive-lib-ffmpeg-contain-66e3cbf8e0:048e95b8bc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-50220df6c1"></a>
- <a id="s-25327f57af"></a>`distribution`: `a-stove0-media-archive-lib`
- <a id="s-462f0b9f29"></a>`module`: `a_stove0_media_archive_lib`
- <a id="s-dca36532a3"></a>`name`: `ffmpeg_container_metadata_args`
- <a id="s-a696f19245"></a>`unit`: `export`

### Declared structure

- <a id="s-5aa548b54a"></a>`kind`: `"function"`
- <a id="s-56209bfaf9"></a>`signature`: `"\"(item: 'MediaProjectionItem') -> 'list[str]'\""`

## Governing policies

- <a id="pa-9288e68c97"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-lib:a_stove0_media_archive_lib](../../../evidence/sources/authorities.md#src-6929294187) — [some-implementations/stove0/targets/media-archive/support/src/a\_stove0\_media\_archive\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/support/src/a_stove0_media_archive_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_lib.ffmpeg_container_metadata_args`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: feadd9dd46b5120f17eb1cb5f017bdc64eaf6aff129fdfd9cf0f1b5af36d7e0d -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(item: 'MediaProjectionItem') -> 'list[str]'\""
  },
  "distribution": "a-stove0-media-archive-lib",
  "module": "a_stove0_media_archive_lib",
  "name": "ffmpeg_container_metadata_args",
  "unit": "export"
}
```

</details>
