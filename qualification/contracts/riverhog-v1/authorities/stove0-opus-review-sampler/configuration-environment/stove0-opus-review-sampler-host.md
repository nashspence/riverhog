# STOVE0_OPUS_REVIEW_SAMPLER_HOST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-opus-review-sampler:stove0-opus-review-sampler-host:7a6417a7d7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-review-sampler](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-653001cc1d"></a>

| Field | Value |
|---|---|
| <a id="s-fd02060d9a"></a>`consumers` | `["stove0-opus-review-sampler"]` |
| <a id="s-0c88846b1b"></a>`default_expressions` | `["'127.0.0.1'"]` |
| <a id="s-aac583777d"></a>`id` | `"stove0-opus-review-sampler:environment:STOVE0_OPUS_REVIEW_SAMPLER_HOST"` |
| <a id="s-4bde8896ce"></a>`input_shape` | `"environment-string"` |
| <a id="s-02d5ecfb28"></a>`name` | `"STOVE0_OPUS_REVIEW_SAMPLER_HOST"` |
| <a id="s-30d31b3b91"></a>`owner` | `"stove0-opus-review-sampler"` |

## Governing policies

- <a id="pa-a8c4169220"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-opus-review-sampler:STOVE0_OPUS_REVIEW_SAMPLER_HOST](../../../evidence/sources.md#src-7e5f631953) — [reference/stove0/targets/opus/review-sampler/src/stove0\_opus\_review\_sampler/app.py::\_parser](../../../../../../reference/stove0/targets/opus/review-sampler/src/stove0_opus_review_sampler/app.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-opus-review-sampler` | [reference/stove0/targets/opus/review-sampler/src/stove0\_opus\_review\_sampler/app.py](../../../../../../reference/stove0/targets/opus/review-sampler/src/stove0_opus_review_sampler/app.py) | `os.getenv(f'{prefix}_HOST', '127.0.0.1')` |

### Machine authority

- `/external_contract/configuration_environment/183`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bd34a1e44817fd998cd82aafcce0e159963d73804b3e900b92a1f2cbd2834ed4 -->

```json
{
  "consumers": [
    "stove0-opus-review-sampler"
  ],
  "default_expressions": [
    "'127.0.0.1'"
  ],
  "id": "stove0-opus-review-sampler:environment:STOVE0_OPUS_REVIEW_SAMPLER_HOST",
  "input_shape": "environment-string",
  "name": "STOVE0_OPUS_REVIEW_SAMPLER_HOST",
  "owner": "stove0-opus-review-sampler"
}
```

</details>
