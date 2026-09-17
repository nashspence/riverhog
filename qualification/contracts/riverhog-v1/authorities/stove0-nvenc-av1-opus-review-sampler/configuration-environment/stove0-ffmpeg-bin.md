# STOVE0_FFMPEG_BIN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-nvenc-av1-opus-review-sampler:stove0-ffmpeg-bin:e60c9fdad1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-review-sampler](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-7243e1c17d"></a>

| Field | Value |
|---|---|
| <a id="s-09bf129595"></a>`consumers` | `["stove0-nvenc-av1-opus-review-sampler"]` |
| <a id="s-a7306188f1"></a>`default_expressions` | `["'ffmpeg'"]` |
| <a id="s-51dcae8466"></a>`id` | `"stove0-nvenc-av1-opus-review-sampler:environment:STOVE0_FFMPEG_BIN"` |
| <a id="s-8d5d306c1a"></a>`input_shape` | `"environment-string"` |
| <a id="s-bbb48e04b5"></a>`name` | `"STOVE0_FFMPEG_BIN"` |
| <a id="s-784160df35"></a>`owner` | `"stove0-nvenc-av1-opus-review-sampler"` |

## Governing policies

- <a id="pa-3981f8b454"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-nvenc-av1-opus-review-sampler:STOVE0_FFMPEG_BIN](../../../evidence/sources.md#src-f77354177a) — [reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0\_nvenc\_av1\_opus\_review\_sampler/app.py::main](../../../../../../reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0_nvenc_av1_opus_review_sampler/app.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-nvenc-av1-opus-review-sampler` | [reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0\_nvenc\_av1\_opus\_review\_sampler/app.py](../../../../../../reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0_nvenc_av1_opus_review_sampler/app.py) | `os.getenv('STOVE0_FFMPEG_BIN', 'ffmpeg')` |

### Machine authority

- `/external_contract/configuration_environment/164`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3b7f22fbb6a7e85dc4f7fc3462a7942c9156e97f84f8e4c37b6a7b07e1f42cd9 -->

```json
{
  "consumers": [
    "stove0-nvenc-av1-opus-review-sampler"
  ],
  "default_expressions": [
    "'ffmpeg'"
  ],
  "id": "stove0-nvenc-av1-opus-review-sampler:environment:STOVE0_FFMPEG_BIN",
  "input_shape": "environment-string",
  "name": "STOVE0_FFMPEG_BIN",
  "owner": "stove0-nvenc-av1-opus-review-sampler"
}
```

</details>
