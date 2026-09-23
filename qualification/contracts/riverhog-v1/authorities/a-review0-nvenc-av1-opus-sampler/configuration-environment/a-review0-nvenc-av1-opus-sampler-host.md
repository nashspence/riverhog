# A_REVIEW0_NVENC_AV1_OPUS_SAMPLER_HOST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-nvenc-av1-opus-sampler:a-review0-nvenc-av1-opus-sampler-host:bacc3c30ef -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-nvenc-av1-opus-sampler](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-1ebfad79c3"></a>

| Field | Value |
|---|---|
| <a id="s-42ed9823cb"></a>`consumers` | `["a-review0-nvenc-av1-opus-sampler"]` |
| <a id="s-9cc68ab77f"></a>`default_expressions` | `["'127.0.0.1'"]` |
| <a id="s-27fcc796bf"></a>`id` | `"a-review0-nvenc-av1-opus-sampler:environment:A_REVIEW0_NVENC_AV1_OPUS_SAMPLER_HOST"` |
| <a id="s-6d9f662f4d"></a>`input_shape` | `"environment-string"` |
| <a id="s-def2479a70"></a>`name` | `"A_REVIEW0_NVENC_AV1_OPUS_SAMPLER_HOST"` |
| <a id="s-18f1726a77"></a>`owner` | `"a-review0-nvenc-av1-opus-sampler"` |

## Governing policies

- <a id="pa-7d09c4a592"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-review0-nvenc-av1-opus-sampler:A_REVIEW0_NVENC_AV1_OPUS_SAMPLER_HOST](../../../evidence/sources/authorities.md#src-720e3e4307) — [some-implementations/stove0/review0/samplers/nvenc-av1-opus/src/a\_review0\_nvenc\_av1\_opus\_sampler/app.py::\_parser](../../../../../../some-implementations/stove0/review0/samplers/nvenc-av1-opus/src/a_review0_nvenc_av1_opus_sampler/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-review0-nvenc-av1-opus-sampler` | [some-implementations/stove0/review0/samplers/nvenc-av1-opus/src/a\_review0\_nvenc\_av1\_opus\_sampler/app.py](../../../../../../some-implementations/stove0/review0/samplers/nvenc-av1-opus/src/a_review0_nvenc_av1_opus_sampler/app.py) | `os.getenv(f'{prefix}_HOST', '127.0.0.1')` |

### Machine authority

- `/external_contract/configuration_environment/15`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3ee5c1a84aaae04cea1f35ae38ed940c8aee0dbe70c04bb369811834d3a75c40 -->

```json
{
  "consumers": [
    "a-review0-nvenc-av1-opus-sampler"
  ],
  "default_expressions": [
    "'127.0.0.1'"
  ],
  "id": "a-review0-nvenc-av1-opus-sampler:environment:A_REVIEW0_NVENC_AV1_OPUS_SAMPLER_HOST",
  "input_shape": "environment-string",
  "name": "A_REVIEW0_NVENC_AV1_OPUS_SAMPLER_HOST",
  "owner": "a-review0-nvenc-av1-opus-sampler"
}
```

</details>
