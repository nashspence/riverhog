# A_RIVERHOG_B2_STORE_ROOT_PREFIX

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-b2-store:a-riverhog-b2-store-root-prefix:0c299aee0f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-b2-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-f34a6099fd"></a>

| Field | Value |
|---|---|
| <a id="s-333cb6b464"></a>`consumers` | `["a-riverhog-b2-store"]` |
| <a id="s-855c76d1b6"></a>`default_expressions` | `["''"]` |
| <a id="s-e45db274e8"></a>`id` | `"a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_ROOT_PREFIX"` |
| <a id="s-6d41d32f3a"></a>`input_shape` | `"environment-string"` |
| <a id="s-e8b92a27c1"></a>`name` | `"A_RIVERHOG_B2_STORE_ROOT_PREFIX"` |
| <a id="s-37c4cf2e50"></a>`owner` | `"a-riverhog-b2-store"` |

## Governing policies

- <a id="pa-70f412579d"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-b2-store:A_RIVERHOG_B2_STORE_ROOT_PREFIX](../../../evidence/sources/authorities.md#src-e29d987bcd) — [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py::\_optional](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-b2-store` | [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py) | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/90`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 053eb1afd354acf954d0653f8e86133ff3a3bb37db1715361f8be3a2d3e0ce0d -->

```json
{
  "consumers": [
    "a-riverhog-b2-store"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_ROOT_PREFIX",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_B2_STORE_ROOT_PREFIX",
  "owner": "a-riverhog-b2-store"
}
```

</details>
