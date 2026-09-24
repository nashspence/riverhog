# a_stove0_media_archive_lib.resolve_media_archive_preflight_projection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-lib:a-stove0-media-archive-lib-resolve-media-d1cd2db4fd:1c88a59961 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-43a49d7f07"></a>
- <a id="s-e576917e7a"></a>`distribution`: `a-stove0-media-archive-lib`
- <a id="s-4f7b30faa9"></a>`module`: `a_stove0_media_archive_lib`
- <a id="s-0d9b7b2481"></a>`name`: `resolve_media_archive_preflight_projection`
- <a id="s-e541150200"></a>`unit`: `export`

### Declared structure

- <a id="s-4cf758231a"></a>`kind`: `"function"`
- <a id="s-a86382d4c4"></a>`signature`: `"\"(request: 'TargetPreflightRequest', *, policy: 'MediaProjectionPolicy', archive_directory: 'str', archive_suffix: 'str') -> 'MediaArchiveProjection'\""`

## Governing policies

- <a id="pa-e45df26600"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-lib:a_stove0_media_archive_lib](../../../evidence/sources/authorities.md#src-6929294187) — [some-implementations/stove0/targets/media-archive/support/src/a\_stove0\_media\_archive\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/support/src/a_stove0_media_archive_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_lib.resolve_media_archive_preflight_projection`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 788e58c9519126c717810d00486517fa1e9066d7adf129094d092df4cc0cd832 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(request: 'TargetPreflightRequest', *, policy: 'MediaProjectionPolicy', archive_directory: 'str', archive_suffix: 'str') -> 'MediaArchiveProjection'\""
  },
  "distribution": "a-stove0-media-archive-lib",
  "module": "a_stove0_media_archive_lib",
  "name": "resolve_media_archive_preflight_projection",
  "unit": "export"
}
```

</details>
