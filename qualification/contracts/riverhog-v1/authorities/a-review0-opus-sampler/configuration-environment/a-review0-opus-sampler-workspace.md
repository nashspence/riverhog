# A_REVIEW0_OPUS_SAMPLER_WORKSPACE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-opus-sampler:a-review0-opus-sampler-workspace:c050416502 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-opus-sampler](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-eb2021dc66"></a>

| Field | Value |
|---|---|
| <a id="s-f72318c871"></a>`consumers` | `["a-review0-opus-sampler"]` |
| <a id="s-c5edc5ccb1"></a>`default_expressions` | `["'/run/review0'"]` |
| <a id="s-f6f255e1f5"></a>`id` | `"a-review0-opus-sampler:environment:A_REVIEW0_OPUS_SAMPLER_WORKSPACE"` |
| <a id="s-546d76419e"></a>`input_shape` | `"environment-string"` |
| <a id="s-b4738a40ad"></a>`name` | `"A_REVIEW0_OPUS_SAMPLER_WORKSPACE"` |
| <a id="s-407a9add94"></a>`owner` | `"a-review0-opus-sampler"` |

## Governing policies

- <a id="pa-7305f77de6"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-review0-opus-sampler:A_REVIEW0_OPUS_SAMPLER_WORKSPACE](../../../evidence/sources/authorities.md#src-79f8e64c86) — [some-implementations/stove0/review0/samplers/opus/src/a\_review0\_opus\_sampler/app.py::main](../../../../../../some-implementations/stove0/review0/samplers/opus/src/a_review0_opus_sampler/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-review0-opus-sampler` | [some-implementations/stove0/review0/samplers/opus/src/a\_review0\_opus\_sampler/app.py](../../../../../../some-implementations/stove0/review0/samplers/opus/src/a_review0_opus_sampler/app.py) | `os.getenv(f'{prefix}_WORKSPACE', '/run/review0')` |

### Machine authority

- `/external_contract/configuration_environment/29`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 960d1cb9aae2e9023cf0427a1beda2e166e54f922ea6ed1b89441b6d71b17e21 -->

```json
{
  "consumers": [
    "a-review0-opus-sampler"
  ],
  "default_expressions": [
    "'/run/review0'"
  ],
  "id": "a-review0-opus-sampler:environment:A_REVIEW0_OPUS_SAMPLER_WORKSPACE",
  "input_shape": "environment-string",
  "name": "A_REVIEW0_OPUS_SAMPLER_WORKSPACE",
  "owner": "a-review0-opus-sampler"
}
```

</details>
