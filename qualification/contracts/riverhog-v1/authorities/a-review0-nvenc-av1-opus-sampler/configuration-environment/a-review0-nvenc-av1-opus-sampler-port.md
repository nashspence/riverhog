# A_REVIEW0_NVENC_AV1_OPUS_SAMPLER_PORT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-nvenc-av1-opus-sampler:a-review0-nvenc-av1-opus-sampler-port:5fa21ca793 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-nvenc-av1-opus-sampler](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-9de9805f44"></a>

| Field | Value |
|---|---|
| <a id="s-f0e22869ab"></a>`consumers` | `["a-review0-nvenc-av1-opus-sampler"]` |
| <a id="s-d192d489c2"></a>`default_expressions` | `["'8080'"]` |
| <a id="s-7c3da5fa0c"></a>`id` | `"a-review0-nvenc-av1-opus-sampler:environment:A_REVIEW0_NVENC_AV1_OPUS_SAMPLER_PORT"` |
| <a id="s-91a7ad7524"></a>`input_shape` | `"environment-string"` |
| <a id="s-81fae89078"></a>`name` | `"A_REVIEW0_NVENC_AV1_OPUS_SAMPLER_PORT"` |
| <a id="s-683687933c"></a>`owner` | `"a-review0-nvenc-av1-opus-sampler"` |

## Governing policies

- <a id="pa-5be405f91f"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-review0-nvenc-av1-opus-sampler:A_REVIEW0_NVENC_AV1_OPUS_SAMPLER_PORT](../../../evidence/sources/authorities.md#src-6071a8d1c6) — [some-implementations/stove0/review0/samplers/nvenc-av1-opus/src/a\_review0\_nvenc\_av1\_opus\_sampler/app.py::\_parser](../../../../../../some-implementations/stove0/review0/samplers/nvenc-av1-opus/src/a_review0_nvenc_av1_opus_sampler/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-review0-nvenc-av1-opus-sampler` | [some-implementations/stove0/review0/samplers/nvenc-av1-opus/src/a\_review0\_nvenc\_av1\_opus\_sampler/app.py](../../../../../../some-implementations/stove0/review0/samplers/nvenc-av1-opus/src/a_review0_nvenc_av1_opus_sampler/app.py) | `os.getenv(f'{prefix}_PORT', '8080')` |

### Machine authority

- `/external_contract/configuration_environment/17`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8b0338aa6e0c6c0f2aa0f4a35de571f3cd8710a0d60d32f48ab01bc26fe13b10 -->

```json
{
  "consumers": [
    "a-review0-nvenc-av1-opus-sampler"
  ],
  "default_expressions": [
    "'8080'"
  ],
  "id": "a-review0-nvenc-av1-opus-sampler:environment:A_REVIEW0_NVENC_AV1_OPUS_SAMPLER_PORT",
  "input_shape": "environment-string",
  "name": "A_REVIEW0_NVENC_AV1_OPUS_SAMPLER_PORT",
  "owner": "a-review0-nvenc-av1-opus-sampler"
}
```

</details>
