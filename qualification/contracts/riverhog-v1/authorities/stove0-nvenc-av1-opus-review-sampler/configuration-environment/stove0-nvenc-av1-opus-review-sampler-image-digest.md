# STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_IMAGE_DIGEST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-nvenc-av1-opus-review-sampler:stove0-nvenc-av1-opus-review-sampler-image-digest:c055949077 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-review-sampler](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](index.md#f-748ed91c55) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-c2d41cc041"></a>
| Field | Shape |
|---|---|
| <a id="s-d7d297bac4"></a>`consumers` | ["stove0-nvenc-av1-opus-review-sampler"] |
| <a id="s-a321f6f65c"></a>`default_expressions` | ["''"] |
| <a id="s-27c5172665"></a>`id` | "stove0-nvenc-av1-opus-review-sampler:environment:STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_IMAGE_DIGEST" |
| <a id="s-3be4ce24d0"></a>`input_shape` | "environment-string" |
| <a id="s-5549d5eeb1"></a>`name` | "STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_IMAGE_DIGEST" |
| <a id="s-c0cbb2767f"></a>`owner` | "stove0-nvenc-av1-opus-review-sampler" |

## Governing policies

- <a id="pa-ef0b9de0e1"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-nvenc-av1-opus-review-sampler:STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_IMAGE_DIGEST](../../../evidence/sources.md#src-669569f115) — `reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0_nvenc_av1_opus_review_sampler/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-nvenc-av1-opus-review-sampler` | `reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0_nvenc_av1_opus_review_sampler/app.py` | `os.getenv(f'{prefix}_IMAGE_DIGEST', '')` |

### Machine authority

- `/external_contract/configuration_environment/166`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a191b6f5475ecd663a402a799d1b69f7caee43b6f21cb64e595805e5aabd11be -->

```json
{
  "consumers": [
    "stove0-nvenc-av1-opus-review-sampler"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "stove0-nvenc-av1-opus-review-sampler:environment:STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_IMAGE_DIGEST",
  "input_shape": "environment-string",
  "name": "STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_IMAGE_DIGEST",
  "owner": "stove0-nvenc-av1-opus-review-sampler"
}
```
