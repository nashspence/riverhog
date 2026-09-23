# A_REVIEW0_OPUS_SAMPLER_IMAGE_DIGEST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-opus-sampler:a-review0-opus-sampler-image-digest:9c1c197d17 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-opus-sampler](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-da42edbdae"></a>

| Field | Value |
|---|---|
| <a id="s-cb8b3d3b8e"></a>`consumers` | `["a-review0-opus-sampler"]` |
| <a id="s-24d8864685"></a>`default_expressions` | `["''"]` |
| <a id="s-c407419be0"></a>`id` | `"a-review0-opus-sampler:environment:A_REVIEW0_OPUS_SAMPLER_IMAGE_DIGEST"` |
| <a id="s-9d5b6c09f0"></a>`input_shape` | `"environment-string"` |
| <a id="s-c08568c77a"></a>`name` | `"A_REVIEW0_OPUS_SAMPLER_IMAGE_DIGEST"` |
| <a id="s-bf0770eba1"></a>`owner` | `"a-review0-opus-sampler"` |

## Governing policies

- <a id="pa-c20c4e3e33"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-review0-opus-sampler:A_REVIEW0_OPUS_SAMPLER_IMAGE_DIGEST](../../../evidence/sources/authorities.md#src-6dfa978333) — [some-implementations/stove0/review0/samplers/opus/src/a\_review0\_opus\_sampler/app.py::\_image\_digest](../../../../../../some-implementations/stove0/review0/samplers/opus/src/a_review0_opus_sampler/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-review0-opus-sampler` | [some-implementations/stove0/review0/samplers/opus/src/a\_review0\_opus\_sampler/app.py](../../../../../../some-implementations/stove0/review0/samplers/opus/src/a_review0_opus_sampler/app.py) | `os.getenv(f'{prefix}_IMAGE_DIGEST', '')` |

### Machine authority

- `/external_contract/configuration_environment/24`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ba1a200f1b5b4ee42804133caa88f543e08157439d423546f8d726a253078340 -->

```json
{
  "consumers": [
    "a-review0-opus-sampler"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-review0-opus-sampler:environment:A_REVIEW0_OPUS_SAMPLER_IMAGE_DIGEST",
  "input_shape": "environment-string",
  "name": "A_REVIEW0_OPUS_SAMPLER_IMAGE_DIGEST",
  "owner": "a-review0-opus-sampler"
}
```

</details>
