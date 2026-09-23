# A_REVIEW0_NVENC_AV1_OPUS_SAMPLER_IMAGE_DIGEST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-nvenc-av1-opus-sampler:a-review0-nvenc-av1-opus-sampler-image-digest:fd14e7d2a7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-nvenc-av1-opus-sampler](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-d9dcf35fac"></a>

| Field | Value |
|---|---|
| <a id="s-10a1d823b2"></a>`consumers` | `["a-review0-nvenc-av1-opus-sampler"]` |
| <a id="s-49794b453e"></a>`default_expressions` | `["''"]` |
| <a id="s-4ed1357761"></a>`id` | `"a-review0-nvenc-av1-opus-sampler:environment:A_REVIEW0_NVENC_AV1_OPUS_SAMPLER_IMAGE_DIGEST"` |
| <a id="s-6a48654def"></a>`input_shape` | `"environment-string"` |
| <a id="s-632be535e2"></a>`name` | `"A_REVIEW0_NVENC_AV1_OPUS_SAMPLER_IMAGE_DIGEST"` |
| <a id="s-0341ad22d1"></a>`owner` | `"a-review0-nvenc-av1-opus-sampler"` |

## Governing policies

- <a id="pa-286238f064"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-review0-nvenc-av1-opus-sampler:A_REVIEW0_NVENC_AV1_OPUS_SAMPLER_IMAGE_DIGEST](../../../evidence/sources/authorities.md#src-809346e967) — [some-implementations/stove0/review0/samplers/nvenc-av1-opus/src/a\_review0\_nvenc\_av1\_opus\_sampler/app.py::\_image\_digest](../../../../../../some-implementations/stove0/review0/samplers/nvenc-av1-opus/src/a_review0_nvenc_av1_opus_sampler/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-review0-nvenc-av1-opus-sampler` | [some-implementations/stove0/review0/samplers/nvenc-av1-opus/src/a\_review0\_nvenc\_av1\_opus\_sampler/app.py](../../../../../../some-implementations/stove0/review0/samplers/nvenc-av1-opus/src/a_review0_nvenc_av1_opus_sampler/app.py) | `os.getenv(f'{prefix}_IMAGE_DIGEST', '')` |

### Machine authority

- `/external_contract/configuration_environment/16`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 58d6aa5b8d20a452249ea5072aba705351b744e15924ef6b9b9f43e6c357352c -->

```json
{
  "consumers": [
    "a-review0-nvenc-av1-opus-sampler"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-review0-nvenc-av1-opus-sampler:environment:A_REVIEW0_NVENC_AV1_OPUS_SAMPLER_IMAGE_DIGEST",
  "input_shape": "environment-string",
  "name": "A_REVIEW0_NVENC_AV1_OPUS_SAMPLER_IMAGE_DIGEST",
  "owner": "a-review0-nvenc-av1-opus-sampler"
}
```

</details>
