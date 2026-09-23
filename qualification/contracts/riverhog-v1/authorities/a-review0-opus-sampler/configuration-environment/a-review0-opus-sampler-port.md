# A_REVIEW0_OPUS_SAMPLER_PORT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-opus-sampler:a-review0-opus-sampler-port:0ff60bb598 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-opus-sampler](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-1b7215e251"></a>

| Field | Value |
|---|---|
| <a id="s-84c48e5200"></a>`consumers` | `["a-review0-opus-sampler"]` |
| <a id="s-4f7fd18cab"></a>`default_expressions` | `["'8080'"]` |
| <a id="s-11b04fc0e6"></a>`id` | `"a-review0-opus-sampler:environment:A_REVIEW0_OPUS_SAMPLER_PORT"` |
| <a id="s-503bc11db4"></a>`input_shape` | `"environment-string"` |
| <a id="s-399d989cc4"></a>`name` | `"A_REVIEW0_OPUS_SAMPLER_PORT"` |
| <a id="s-03a4e23000"></a>`owner` | `"a-review0-opus-sampler"` |

## Governing policies

- <a id="pa-30b094a529"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-review0-opus-sampler:A_REVIEW0_OPUS_SAMPLER_PORT](../../../evidence/sources/authorities.md#src-1786709c18) — [some-implementations/stove0/review0/samplers/opus/src/a\_review0\_opus\_sampler/app.py::\_parser](../../../../../../some-implementations/stove0/review0/samplers/opus/src/a_review0_opus_sampler/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-review0-opus-sampler` | [some-implementations/stove0/review0/samplers/opus/src/a\_review0\_opus\_sampler/app.py](../../../../../../some-implementations/stove0/review0/samplers/opus/src/a_review0_opus_sampler/app.py) | `os.getenv(f'{prefix}_PORT', '8080')` |

### Machine authority

- `/external_contract/configuration_environment/25`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 85cbe4ade520b31e2c04be8073e117fe7dd9c2750baa4667e3535cfef7ff1a3b -->

```json
{
  "consumers": [
    "a-review0-opus-sampler"
  ],
  "default_expressions": [
    "'8080'"
  ],
  "id": "a-review0-opus-sampler:environment:A_REVIEW0_OPUS_SAMPLER_PORT",
  "input_shape": "environment-string",
  "name": "A_REVIEW0_OPUS_SAMPLER_PORT",
  "owner": "a-review0-opus-sampler"
}
```

</details>
