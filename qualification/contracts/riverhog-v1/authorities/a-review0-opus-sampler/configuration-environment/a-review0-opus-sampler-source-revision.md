# A_REVIEW0_OPUS_SAMPLER_SOURCE_REVISION

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-opus-sampler:a-review0-opus-sampler-source-revision:471b58bae3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-opus-sampler](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-b211a29ced"></a>

| Field | Value |
|---|---|
| <a id="s-9c278af545"></a>`consumers` | `["a-review0-opus-sampler"]` |
| <a id="s-ccc7b8f4c0"></a>`default_expressions` | `["'unknown'"]` |
| <a id="s-d0c4986063"></a>`id` | `"a-review0-opus-sampler:environment:A_REVIEW0_OPUS_SAMPLER_SOURCE_REVISION"` |
| <a id="s-8f8f263604"></a>`input_shape` | `"environment-string"` |
| <a id="s-18bbe5ebd0"></a>`name` | `"A_REVIEW0_OPUS_SAMPLER_SOURCE_REVISION"` |
| <a id="s-2f1faef8a9"></a>`owner` | `"a-review0-opus-sampler"` |

## Governing policies

- <a id="pa-2b2a072fb3"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-review0-opus-sampler:A_REVIEW0_OPUS_SAMPLER_SOURCE_REVISION](../../../evidence/sources/authorities.md#src-9b4e102905) — [some-implementations/stove0/review0/samplers/opus/src/a\_review0\_opus\_sampler/app.py::main](../../../../../../some-implementations/stove0/review0/samplers/opus/src/a_review0_opus_sampler/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-review0-opus-sampler` | [some-implementations/stove0/review0/samplers/opus/src/a\_review0\_opus\_sampler/app.py](../../../../../../some-implementations/stove0/review0/samplers/opus/src/a_review0_opus_sampler/app.py) | `os.getenv(f'{prefix}_SOURCE_REVISION', 'unknown')` |

### Machine authority

- `/external_contract/configuration_environment/23`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8c5d5a583b9d73321fb106964969f917a14f6c999c0ba53b2a461dd165443acd -->

```json
{
  "consumers": [
    "a-review0-opus-sampler"
  ],
  "default_expressions": [
    "'unknown'"
  ],
  "id": "a-review0-opus-sampler:environment:A_REVIEW0_OPUS_SAMPLER_SOURCE_REVISION",
  "input_shape": "environment-string",
  "name": "A_REVIEW0_OPUS_SAMPLER_SOURCE_REVISION",
  "owner": "a-review0-opus-sampler"
}
```

</details>
