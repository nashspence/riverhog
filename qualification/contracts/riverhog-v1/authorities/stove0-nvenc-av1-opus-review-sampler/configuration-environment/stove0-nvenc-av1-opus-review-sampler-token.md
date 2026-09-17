# STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-nvenc-av1-opus-review-sampler:stove0-nvenc-av1-opus-review-sampler-token:ce5cfcc743 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-review-sampler](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-ca94803e2e"></a>

| Field | Value |
|---|---|
| <a id="s-1275b3d711"></a>`consumers` | `["stove0-nvenc-av1-opus-review-sampler"]` |
| <a id="s-1d87666af8"></a>`default_expressions` | `["unset"]` |
| <a id="s-2f5fd47083"></a>`id` | `"stove0-nvenc-av1-opus-review-sampler:environment:STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_TOKEN"` |
| <a id="s-2e313a89c7"></a>`input_shape` | `"environment-string"` |
| <a id="s-cb11eb4d9d"></a>`name` | `"STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_TOKEN"` |
| <a id="s-c58778d45d"></a>`owner` | `"stove0-nvenc-av1-opus-review-sampler"` |

## Governing policies

- <a id="pa-cc3001a6be"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-nvenc-av1-opus-review-sampler:STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_TOKEN](../../../evidence/sources.md#src-c3e5740197) — [reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0\_nvenc\_av1\_opus\_review\_sampler/app.py::\_secret](../../../../../../reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0_nvenc_av1_opus_review_sampler/app.py); [reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0\_nvenc\_av1\_opus\_review\_sampler/app.py::main](../../../../../../reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0_nvenc_av1_opus_review_sampler/app.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-nvenc-av1-opus-review-sampler` | [reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0\_nvenc\_av1\_opus\_review\_sampler/app.py](../../../../../../reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0_nvenc_av1_opus_review_sampler/app.py) | `os.getenv(f'{prefix}_TOKEN')` |
| parser | `stove0-nvenc-av1-opus-review-sampler` | [reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0\_nvenc\_av1\_opus\_review\_sampler/app.py](../../../../../../reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0_nvenc_av1_opus_review_sampler/app.py) | `os.environ.pop(f'{prefix}_TOKEN')` |

### Machine authority

- `/external_contract/configuration_environment/169`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c5a2e5b9b808db6f43b1c13b8c5d4d46023b6d66af6add149b6c8a8d03721bd5 -->

```json
{
  "consumers": [
    "stove0-nvenc-av1-opus-review-sampler"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "stove0-nvenc-av1-opus-review-sampler:environment:STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_TOKEN",
  "input_shape": "environment-string",
  "name": "STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_TOKEN",
  "owner": "stove0-nvenc-av1-opus-review-sampler"
}
```

</details>
