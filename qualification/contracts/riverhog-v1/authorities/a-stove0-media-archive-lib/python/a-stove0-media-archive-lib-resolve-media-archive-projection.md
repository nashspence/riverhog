# a_stove0_media_archive_lib.resolve_media_archive_projection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-lib:a-stove0-media-archive-lib-resolve-media-e5a7a123c4:a13be9656b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b36b0129a1"></a>
- <a id="s-f965e491c3"></a>`distribution`: `a-stove0-media-archive-lib`
- <a id="s-7ed7306046"></a>`module`: `a_stove0_media_archive_lib`
- <a id="s-836fdb5ea1"></a>`name`: `resolve_media_archive_projection`
- <a id="s-07a0e4accf"></a>`unit`: `export`

### Declared structure

- <a id="s-9270d0bd5b"></a>`kind`: `"function"`
- <a id="s-7962645f2e"></a>`signature`: `"\"(*, inputs: 'Sequence[InputArtifact]', observations: 'Sequence[ContentObservationEvidence]', policy: 'MediaProjectionPolicy', archive_directory: 'str', archive_suffix: 'str') -> 'MediaArchiveProjection'\""`

## Governing policies

- <a id="pa-c1164fa406"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-lib:a_stove0_media_archive_lib](../../../evidence/sources/authorities.md#src-6929294187) — [some-implementations/stove0/targets/media-archive/support/src/a\_stove0\_media\_archive\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/support/src/a_stove0_media_archive_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_lib.resolve_media_archive_projection`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d8db9008cb397ccbf752061b43dd56ebf77f55b4a272ff3afba2bfd238af2952 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, inputs: 'Sequence[InputArtifact]', observations: 'Sequence[ContentObservationEvidence]', policy: 'MediaProjectionPolicy', archive_directory: 'str', archive_suffix: 'str') -> 'MediaArchiveProjection'\""
  },
  "distribution": "a-stove0-media-archive-lib",
  "module": "a_stove0_media_archive_lib",
  "name": "resolve_media_archive_projection",
  "unit": "export"
}
```

</details>
