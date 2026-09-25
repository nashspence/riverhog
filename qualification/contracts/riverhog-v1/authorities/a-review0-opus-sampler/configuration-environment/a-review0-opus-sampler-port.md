# A_REVIEW0_OPUS_SAMPLER_PORT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-opus-sampler:a-review0-opus-sampler-port:3aa4ae834c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-opus-sampler](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-f3c5c446af"></a>

| Field | Value |
|---|---|
| <a id="s-e5f41d64e9"></a>`consumers` | `["a-review0-opus-sampler"]` |
| <a id="s-ee3ab728a9"></a>`default_expressions` | `["'8080'"]` |
| <a id="s-4f2d62dbc3"></a>`id` | `"a-review0-opus-sampler:environment:A_REVIEW0_OPUS_SAMPLER_PORT"` |
| <a id="s-63e22c344b"></a>`input_shape` | `"environment-string"` |
| <a id="s-034b439c45"></a>`name` | `"A_REVIEW0_OPUS_SAMPLER_PORT"` |
| <a id="s-5b255b15bc"></a>`owner` | `"a-review0-opus-sampler"` |

## Governing policies

- <a id="pa-791b5f5caa"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

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

- `/external_contract/configuration_environment/22`

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
