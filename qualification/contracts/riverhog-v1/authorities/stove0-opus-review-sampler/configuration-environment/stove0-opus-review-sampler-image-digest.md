# STOVE0_OPUS_REVIEW_SAMPLER_IMAGE_DIGEST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-opus-review-sampler:stove0-opus-review-sampler-image-digest:e5970cf6cf -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-review-sampler](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-4d873f87c7"></a>

| Field | Value |
|---|---|
| <a id="s-1b512f4642"></a>`consumers` | `["stove0-opus-review-sampler"]` |
| <a id="s-4a866868af"></a>`default_expressions` | `["''"]` |
| <a id="s-1c5e68fc52"></a>`id` | `"stove0-opus-review-sampler:environment:STOVE0_OPUS_REVIEW_SAMPLER_IMAGE_DIGEST"` |
| <a id="s-9c30e58ba4"></a>`input_shape` | `"environment-string"` |
| <a id="s-a11372d7df"></a>`name` | `"STOVE0_OPUS_REVIEW_SAMPLER_IMAGE_DIGEST"` |
| <a id="s-ea481a7a25"></a>`owner` | `"stove0-opus-review-sampler"` |

## Governing policies

- <a id="pa-87a297bb10"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-opus-review-sampler:STOVE0_OPUS_REVIEW_SAMPLER_IMAGE_DIGEST](../../../evidence/sources/authorities.md#src-7dcd2e53f5) — [reference/stove0/targets/opus/review-sampler/src/stove0\_opus\_review\_sampler/app.py::\_image\_digest](../../../../../../reference/stove0/targets/opus/review-sampler/src/stove0_opus_review_sampler/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-opus-review-sampler` | [reference/stove0/targets/opus/review-sampler/src/stove0\_opus\_review\_sampler/app.py](../../../../../../reference/stove0/targets/opus/review-sampler/src/stove0_opus_review_sampler/app.py) | `os.getenv(f'{prefix}_IMAGE_DIGEST', '')` |

### Machine authority

- `/external_contract/configuration_environment/184`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0f40916296daf91b2b604b2f80ee8c9338f9260b39f5e50c6511c87e238717b2 -->

```json
{
  "consumers": [
    "stove0-opus-review-sampler"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "stove0-opus-review-sampler:environment:STOVE0_OPUS_REVIEW_SAMPLER_IMAGE_DIGEST",
  "input_shape": "environment-string",
  "name": "STOVE0_OPUS_REVIEW_SAMPLER_IMAGE_DIGEST",
  "owner": "stove0-opus-review-sampler"
}
```

</details>
