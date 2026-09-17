# STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_WORKSPACE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-nvenc-av1-opus-review-sampler:stove0-nvenc-av1-opus-review-sampler-workspace:9f5ac66f54 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-review-sampler](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-60d36f56ec"></a>

| Field | Value |
|---|---|
| <a id="s-6a75627f71"></a>`consumers` | `["stove0-nvenc-av1-opus-review-sampler"]` |
| <a id="s-7f0cb5f08e"></a>`default_expressions` | `["'/run/stove0-review'"]` |
| <a id="s-9855b52610"></a>`id` | `"stove0-nvenc-av1-opus-review-sampler:environment:STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_WORKSPACE"` |
| <a id="s-93a8c602ad"></a>`input_shape` | `"environment-string"` |
| <a id="s-25d67a7319"></a>`name` | `"STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_WORKSPACE"` |
| <a id="s-1c9291919b"></a>`owner` | `"stove0-nvenc-av1-opus-review-sampler"` |

## Governing policies

- <a id="pa-e127a83b41"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-nvenc-av1-opus-review-sampler:STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_WORKSPACE](../../../evidence/sources.md#src-6b93ebbc94) — [reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0\_nvenc\_av1\_opus\_review\_sampler/app.py::main](../../../../../../reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0_nvenc_av1_opus_review_sampler/app.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-nvenc-av1-opus-review-sampler` | [reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0\_nvenc\_av1\_opus\_review\_sampler/app.py](../../../../../../reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0_nvenc_av1_opus_review_sampler/app.py) | `os.getenv(f'{prefix}_WORKSPACE', '/run/stove0-review')` |

### Machine authority

- `/external_contract/configuration_environment/171`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b2c09cf3ab6cee8acc98ca1018173296c4f4e7b808d902c2baaa4987746d060b -->

```json
{
  "consumers": [
    "stove0-nvenc-av1-opus-review-sampler"
  ],
  "default_expressions": [
    "'/run/stove0-review'"
  ],
  "id": "stove0-nvenc-av1-opus-review-sampler:environment:STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_WORKSPACE",
  "input_shape": "environment-string",
  "name": "STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_WORKSPACE",
  "owner": "stove0-nvenc-av1-opus-review-sampler"
}
```

</details>
