# A_REVIEW0_OPUS_SAMPLER_HOST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-opus-sampler:a-review0-opus-sampler-host:491eba2c17 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-opus-sampler](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-c9bbf6ae4d"></a>

| Field | Value |
|---|---|
| <a id="s-f8e9201576"></a>`consumers` | `["a-review0-opus-sampler"]` |
| <a id="s-d4eff3f5c1"></a>`default_expressions` | `["'127.0.0.1'"]` |
| <a id="s-cf2890bc74"></a>`id` | `"a-review0-opus-sampler:environment:A_REVIEW0_OPUS_SAMPLER_HOST"` |
| <a id="s-d5025fbc5e"></a>`input_shape` | `"environment-string"` |
| <a id="s-3ea82a14dd"></a>`name` | `"A_REVIEW0_OPUS_SAMPLER_HOST"` |
| <a id="s-12f813c67b"></a>`owner` | `"a-review0-opus-sampler"` |

## Governing policies

- <a id="pa-071ea74866"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-review0-opus-sampler:A_REVIEW0_OPUS_SAMPLER_HOST](../../../evidence/sources/authorities.md#src-08e05a0be6) — [some-implementations/stove0/review0/samplers/opus/src/a\_review0\_opus\_sampler/app.py::\_parser](../../../../../../some-implementations/stove0/review0/samplers/opus/src/a_review0_opus_sampler/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-review0-opus-sampler` | [some-implementations/stove0/review0/samplers/opus/src/a\_review0\_opus\_sampler/app.py](../../../../../../some-implementations/stove0/review0/samplers/opus/src/a_review0_opus_sampler/app.py) | `os.getenv(f'{prefix}_HOST', '127.0.0.1')` |

### Machine authority

- `/external_contract/configuration_environment/20`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1d0383b7bbcb8dd0af6936f8d8a0745ff47b26fc3bb3c2f1e30347b70a202c03 -->

```json
{
  "consumers": [
    "a-review0-opus-sampler"
  ],
  "default_expressions": [
    "'127.0.0.1'"
  ],
  "id": "a-review0-opus-sampler:environment:A_REVIEW0_OPUS_SAMPLER_HOST",
  "input_shape": "environment-string",
  "name": "A_REVIEW0_OPUS_SAMPLER_HOST",
  "owner": "a-review0-opus-sampler"
}
```

</details>
