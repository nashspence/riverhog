# STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_HOST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-nvenc-av1-opus-review-sampler:stove0-nvenc-av1-opus-review-sampler-host:8b050356f3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-review-sampler](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-cfbaea4326"></a>
| Field | Shape |
|---|---|
| <a id="s-5fc0320eea"></a>`consumers` | ["stove0-nvenc-av1-opus-review-sampler"] |
| <a id="s-16cbc0bf7b"></a>`default_expressions` | ["'127.0.0.1'"] |
| <a id="s-4fbc0b16b3"></a>`id` | "stove0-nvenc-av1-opus-review-sampler:environment:STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_HOST" |
| <a id="s-25498719da"></a>`input_shape` | "environment-string" |
| <a id="s-4a5e648bd1"></a>`name` | "STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_HOST" |
| <a id="s-e1b146d1e8"></a>`owner` | "stove0-nvenc-av1-opus-review-sampler" |

## Governing policies

- <a id="pa-954f2bd538"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-nvenc-av1-opus-review-sampler:STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_HOST](../../../evidence/sources.md#src-bfa433528a) — `reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0_nvenc_av1_opus_review_sampler/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-nvenc-av1-opus-review-sampler` | `reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0_nvenc_av1_opus_review_sampler/app.py` | `os.getenv(f'{prefix}_HOST', '127.0.0.1')` |

### Machine authority

- `/external_contract/configuration_environment/165`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3296bb5d479be9cb6c172c6c215f554e6feaf40690503d2f3167b17dacbea2fd -->

```json
{
  "consumers": [
    "stove0-nvenc-av1-opus-review-sampler"
  ],
  "default_expressions": [
    "'127.0.0.1'"
  ],
  "id": "stove0-nvenc-av1-opus-review-sampler:environment:STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_HOST",
  "input_shape": "environment-string",
  "name": "STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_HOST",
  "owner": "stove0-nvenc-av1-opus-review-sampler"
}
```
