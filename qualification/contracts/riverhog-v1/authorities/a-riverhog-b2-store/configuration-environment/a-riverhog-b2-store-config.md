# A_RIVERHOG_B2_STORE_CONFIG

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-b2-store:a-riverhog-b2-store-config:39e28fe59a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-b2-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-71ed3f559b"></a>

| Field | Value |
|---|---|
| <a id="s-93ede9eab6"></a>`consumers` | `["a-riverhog-b2-store"]` |
| <a id="s-5513911e29"></a>`default_expressions` | `["unset"]` |
| <a id="s-b5799a6efe"></a>`id` | `"a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_CONFIG"` |
| <a id="s-12fb5098c2"></a>`input_shape` | `"environment-string"` |
| <a id="s-a3d7c530e4"></a>`name` | `"A_RIVERHOG_B2_STORE_CONFIG"` |
| <a id="s-23d7da2e59"></a>`owner` | `"a-riverhog-b2-store"` |

## Governing policies

- <a id="pa-739f41e258"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-b2-store:A_RIVERHOG_B2_STORE_CONFIG](../../../evidence/sources/authorities.md#src-89c980aa41) — [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py::\_parser](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-b2-store` | [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py) | `os.getenv(f'{_PREFIX}CONFIG')` |

### Machine authority

- `/external_contract/configuration_environment/39`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 816e013526190732a890994258490c065ac85e39d6fbe6ca023af32b4c8c7c82 -->

```json
{
  "consumers": [
    "a-riverhog-b2-store"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_CONFIG",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_B2_STORE_CONFIG",
  "owner": "a-riverhog-b2-store"
}
```

</details>
