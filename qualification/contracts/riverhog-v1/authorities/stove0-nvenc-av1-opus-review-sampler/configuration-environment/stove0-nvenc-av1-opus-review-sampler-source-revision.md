# STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_SOURCE_REVISION

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-nvenc-av1-opus-review-sampler:stove0-nvenc-av1-opus-review-sampler-source-revision:07074b0e7d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-review-sampler](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-522054ba96"></a>

| Field | Value |
|---|---|
| <a id="s-10b1bd0fd7"></a>`consumers` | `["stove0-nvenc-av1-opus-review-sampler"]` |
| <a id="s-4987bd6783"></a>`default_expressions` | `["'unknown'"]` |
| <a id="s-881365a784"></a>`id` | `"stove0-nvenc-av1-opus-review-sampler:environment:STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_SOURCE_REVISION"` |
| <a id="s-43682c84fe"></a>`input_shape` | `"environment-string"` |
| <a id="s-020147690b"></a>`name` | `"STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_SOURCE_REVISION"` |
| <a id="s-3bb548651c"></a>`owner` | `"stove0-nvenc-av1-opus-review-sampler"` |

## Governing policies

- <a id="pa-97f4fe1428"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-nvenc-av1-opus-review-sampler:STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_SOURCE_REVISION](../../../evidence/sources.md#src-8dd2815ac9) — `reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0_nvenc_av1_opus_review_sampler/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-nvenc-av1-opus-review-sampler` | `reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0_nvenc_av1_opus_review_sampler/app.py` | `os.getenv(f'{prefix}_SOURCE_REVISION', 'unknown')` |

### Machine authority

- `/external_contract/configuration_environment/168`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 46e552ca3b5c2eadb614bc0c3935fa1d5495d289a4d4fc1ddcd60a9dbf3374a8 -->

```json
{
  "consumers": [
    "stove0-nvenc-av1-opus-review-sampler"
  ],
  "default_expressions": [
    "'unknown'"
  ],
  "id": "stove0-nvenc-av1-opus-review-sampler:environment:STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_SOURCE_REVISION",
  "input_shape": "environment-string",
  "name": "STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_SOURCE_REVISION",
  "owner": "stove0-nvenc-av1-opus-review-sampler"
}
```

</details>
