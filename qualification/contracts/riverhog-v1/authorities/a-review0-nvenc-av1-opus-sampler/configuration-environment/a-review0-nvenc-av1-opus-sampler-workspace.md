# A_REVIEW0_NVENC_AV1_OPUS_SAMPLER_WORKSPACE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-nvenc-av1-opus-sampler:a-review0-nvenc-av1-opus-sampler-workspace:70d22c1f1e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-nvenc-av1-opus-sampler](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-20d8c220d5"></a>

| Field | Value |
|---|---|
| <a id="s-afe2b91816"></a>`consumers` | `["a-review0-nvenc-av1-opus-sampler"]` |
| <a id="s-fd6337595d"></a>`default_expressions` | `["'/run/review0'"]` |
| <a id="s-26e4801367"></a>`id` | `"a-review0-nvenc-av1-opus-sampler:environment:A_REVIEW0_NVENC_AV1_OPUS_SAMPLER_WORKSPACE"` |
| <a id="s-8ec763253d"></a>`input_shape` | `"environment-string"` |
| <a id="s-c40791ce1d"></a>`name` | `"A_REVIEW0_NVENC_AV1_OPUS_SAMPLER_WORKSPACE"` |
| <a id="s-e151128158"></a>`owner` | `"a-review0-nvenc-av1-opus-sampler"` |

## Governing policies

- <a id="pa-e7bfd279ae"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-review0-nvenc-av1-opus-sampler:A_REVIEW0_NVENC_AV1_OPUS_SAMPLER_WORKSPACE](../../../evidence/sources/authorities.md#src-d46f4e92fd) — [some-implementations/stove0/review0/samplers/nvenc-av1-opus/src/a\_review0\_nvenc\_av1\_opus\_sampler/app.py::main](../../../../../../some-implementations/stove0/review0/samplers/nvenc-av1-opus/src/a_review0_nvenc_av1_opus_sampler/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-review0-nvenc-av1-opus-sampler` | [some-implementations/stove0/review0/samplers/nvenc-av1-opus/src/a\_review0\_nvenc\_av1\_opus\_sampler/app.py](../../../../../../some-implementations/stove0/review0/samplers/nvenc-av1-opus/src/a_review0_nvenc_av1_opus_sampler/app.py) | `os.getenv(f'{prefix}_WORKSPACE', '/run/review0')` |

### Machine authority

- `/external_contract/configuration_environment/21`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 00cb718d87b1219bc8ab3e5cbc7ee4659b89561cf7fef070b1117a79f21185b1 -->

```json
{
  "consumers": [
    "a-review0-nvenc-av1-opus-sampler"
  ],
  "default_expressions": [
    "'/run/review0'"
  ],
  "id": "a-review0-nvenc-av1-opus-sampler:environment:A_REVIEW0_NVENC_AV1_OPUS_SAMPLER_WORKSPACE",
  "input_shape": "environment-string",
  "name": "A_REVIEW0_NVENC_AV1_OPUS_SAMPLER_WORKSPACE",
  "owner": "a-review0-nvenc-av1-opus-sampler"
}
```

</details>
