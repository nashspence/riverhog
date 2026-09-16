# STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_TOKEN_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-nvenc-av1-opus-review-sampler:stove0-nvenc-av1-opus-review-sampler-token-file:7a6eeb37d1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-review-sampler](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-816e37960e"></a>

| Field | Value |
|---|---|
| <a id="s-fa4c05bb12"></a>`consumers` | `["stove0-nvenc-av1-opus-review-sampler"]` |
| <a id="s-4e62e91c47"></a>`default_expressions` | `["unset"]` |
| <a id="s-06a4b80139"></a>`id` | `"stove0-nvenc-av1-opus-review-sampler:environment:STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_TOKEN_FILE"` |
| <a id="s-3e8d3974d9"></a>`input_shape` | `"environment-string"` |
| <a id="s-f29bb301cb"></a>`name` | `"STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_TOKEN_FILE"` |
| <a id="s-07e6e69a7f"></a>`owner` | `"stove0-nvenc-av1-opus-review-sampler"` |

## Governing policies

- <a id="pa-cebb6b321a"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-nvenc-av1-opus-review-sampler:STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_TOKEN_FILE](../../../evidence/sources.md#src-0c482b4895) — `reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0_nvenc_av1_opus_review_sampler/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-nvenc-av1-opus-review-sampler` | `reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0_nvenc_av1_opus_review_sampler/app.py` | `os.getenv(f'{prefix}_TOKEN_FILE')` |

### Machine authority

- `/external_contract/configuration_environment/170`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7eae6ad6dcaac9b62077ba723a1ca86285babfda7feeeccea92c46c762f62d88 -->

```json
{
  "consumers": [
    "stove0-nvenc-av1-opus-review-sampler"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "stove0-nvenc-av1-opus-review-sampler:environment:STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_TOKEN_FILE",
  "input_shape": "environment-string",
  "name": "STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_TOKEN_FILE",
  "owner": "stove0-nvenc-av1-opus-review-sampler"
}
```

</details>
