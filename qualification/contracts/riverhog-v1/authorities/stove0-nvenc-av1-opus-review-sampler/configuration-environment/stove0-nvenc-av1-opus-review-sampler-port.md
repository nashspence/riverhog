# STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_PORT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-nvenc-av1-opus-review-sampler:stove0-nvenc-av1-opus-review-sampler-port:cd52b53800 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-review-sampler](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-93311950fd"></a>

| Field | Value |
|---|---|
| <a id="s-2f4d41b4b3"></a>`consumers` | `["stove0-nvenc-av1-opus-review-sampler"]` |
| <a id="s-1ab26ba46c"></a>`default_expressions` | `["'8080'"]` |
| <a id="s-3f335b0525"></a>`id` | `"stove0-nvenc-av1-opus-review-sampler:environment:STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_PORT"` |
| <a id="s-f415c8792e"></a>`input_shape` | `"environment-string"` |
| <a id="s-5f10e92d26"></a>`name` | `"STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_PORT"` |
| <a id="s-ca63a482d2"></a>`owner` | `"stove0-nvenc-av1-opus-review-sampler"` |

## Governing policies

- <a id="pa-e29220387b"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-nvenc-av1-opus-review-sampler:STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_PORT](../../../evidence/sources.md#src-aab9b1006d) — `reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0_nvenc_av1_opus_review_sampler/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-nvenc-av1-opus-review-sampler` | `reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0_nvenc_av1_opus_review_sampler/app.py` | `os.getenv(f'{prefix}_PORT', '8080')` |

### Machine authority

- `/external_contract/configuration_environment/167`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a00022f6b4ca7b9f607e1bf90bf53298326396716740edcfe6e4719ab26c542b -->

```json
{
  "consumers": [
    "stove0-nvenc-av1-opus-review-sampler"
  ],
  "default_expressions": [
    "'8080'"
  ],
  "id": "stove0-nvenc-av1-opus-review-sampler:environment:STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_PORT",
  "input_shape": "environment-string",
  "name": "STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_PORT",
  "owner": "stove0-nvenc-av1-opus-review-sampler"
}
```

</details>
