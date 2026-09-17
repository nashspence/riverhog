# stove0_media_archive_target_support.resolve_media_archive_projection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-support:stove0-media-archive-target-support-resol-9bb967c635:03c09da36f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-585f8335bb"></a>
- <a id="s-99737cbd14"></a>`distribution`: `stove0-media-archive-target-support`
- <a id="s-dad3198b4c"></a>`module`: `stove0_media_archive_target_support`
- <a id="s-ffce416e15"></a>`name`: `resolve_media_archive_projection`
- <a id="s-46fac491cd"></a>`unit`: `export`

### Declared structure

- <a id="s-f2d7ea5079"></a>`kind`: `"function"`
- <a id="s-fa1d001476"></a>`signature`: `"\"(*, inputs: 'Sequence[InputArtifact]', observations: 'Sequence[ObservationEvidence]', policy: 'MediaProjectionPolicy', archive_directory: 'str', archive_suffix: 'str') -> 'MediaArchiveProjection'\""`

## Governing policies

- <a id="pa-82c8e8eac1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-media-archive-target-support:stove0_media_archive_target_support](../../../evidence/sources.md#src-3caa343b1d) — [reference/stove0/targets/media-archive/support/src/stove0\_media\_archive\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/targets/media-archive/support/src/stove0_media_archive_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_media_archive_target_support.resolve_media_archive_projection`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a96bdf7c29171c318ebb85f8514e0252d84825e8d59e955385176327bf1884bd -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, inputs: 'Sequence[InputArtifact]', observations: 'Sequence[ObservationEvidence]', policy: 'MediaProjectionPolicy', archive_directory: 'str', archive_suffix: 'str') -> 'MediaArchiveProjection'\""
  },
  "distribution": "stove0-media-archive-target-support",
  "module": "stove0_media_archive_target_support",
  "name": "resolve_media_archive_projection",
  "unit": "export"
}
```

</details>
