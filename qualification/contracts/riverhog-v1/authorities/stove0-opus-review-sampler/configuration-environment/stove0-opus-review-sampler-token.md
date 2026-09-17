# STOVE0_OPUS_REVIEW_SAMPLER_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-opus-review-sampler:stove0-opus-review-sampler-token:bf2d44a28c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-review-sampler](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-82450fb00f"></a>

| Field | Value |
|---|---|
| <a id="s-d73c536259"></a>`consumers` | `["stove0-opus-review-sampler"]` |
| <a id="s-dc224ebacb"></a>`default_expressions` | `["unset"]` |
| <a id="s-16d235080a"></a>`id` | `"stove0-opus-review-sampler:environment:STOVE0_OPUS_REVIEW_SAMPLER_TOKEN"` |
| <a id="s-a9b457de03"></a>`input_shape` | `"environment-string"` |
| <a id="s-143d7ec0d5"></a>`name` | `"STOVE0_OPUS_REVIEW_SAMPLER_TOKEN"` |
| <a id="s-be5f57ae50"></a>`owner` | `"stove0-opus-review-sampler"` |

## Governing policies

- <a id="pa-461931683b"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-opus-review-sampler:STOVE0_OPUS_REVIEW_SAMPLER_TOKEN](../../../evidence/sources.md#src-3e7ab7516f) — [reference/stove0/targets/opus/review-sampler/src/stove0\_opus\_review\_sampler/app.py::\_secret](../../../../../../reference/stove0/targets/opus/review-sampler/src/stove0_opus_review_sampler/app.py); [reference/stove0/targets/opus/review-sampler/src/stove0\_opus\_review\_sampler/app.py::main](../../../../../../reference/stove0/targets/opus/review-sampler/src/stove0_opus_review_sampler/app.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-opus-review-sampler` | [reference/stove0/targets/opus/review-sampler/src/stove0\_opus\_review\_sampler/app.py](../../../../../../reference/stove0/targets/opus/review-sampler/src/stove0_opus_review_sampler/app.py) | `os.getenv(f'{prefix}_TOKEN')` |
| parser | `stove0-opus-review-sampler` | [reference/stove0/targets/opus/review-sampler/src/stove0\_opus\_review\_sampler/app.py](../../../../../../reference/stove0/targets/opus/review-sampler/src/stove0_opus_review_sampler/app.py) | `os.environ.pop(f'{prefix}_TOKEN')` |

### Machine authority

- `/external_contract/configuration_environment/187`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 94964f04258fe6cc883adf73c564d06b34949a2adbbddcfae759d34bbaf0cc66 -->

```json
{
  "consumers": [
    "stove0-opus-review-sampler"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "stove0-opus-review-sampler:environment:STOVE0_OPUS_REVIEW_SAMPLER_TOKEN",
  "input_shape": "environment-string",
  "name": "STOVE0_OPUS_REVIEW_SAMPLER_TOKEN",
  "owner": "stove0-opus-review-sampler"
}
```

</details>
