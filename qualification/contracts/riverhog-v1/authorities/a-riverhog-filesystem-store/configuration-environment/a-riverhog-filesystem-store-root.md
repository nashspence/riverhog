# A_RIVERHOG_FILESYSTEM_STORE_ROOT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-filesystem-store:a-riverhog-filesystem-store-root:3fd8e8c650 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-filesystem-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-1cd6fe0a97"></a>

| Field | Value |
|---|---|
| <a id="s-923821698e"></a>`consumers` | `["a-riverhog-filesystem-store"]` |
| <a id="s-31873541f2"></a>`default_expressions` | `["''"]` |
| <a id="s-a7a2756f2e"></a>`id` | `"a-riverhog-filesystem-store:environment:A_RIVERHOG_FILESYSTEM_STORE_ROOT"` |
| <a id="s-f7fb27f6db"></a>`input_shape` | `"environment-string"` |
| <a id="s-8f98162f43"></a>`name` | `"A_RIVERHOG_FILESYSTEM_STORE_ROOT"` |
| <a id="s-5f637bee10"></a>`owner` | `"a-riverhog-filesystem-store"` |

## Governing policies

- <a id="pa-c3dbdd0b32"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-filesystem-store:A_RIVERHOG_FILESYSTEM_STORE_ROOT](../../../evidence/sources/authorities.md#src-090033eeb5) — [some-implementations/riverhog/storage/filesystem/src/a\_riverhog\_filesystem\_store/app.py::\_required](../../../../../../some-implementations/riverhog/storage/filesystem/src/a_riverhog_filesystem_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-filesystem-store` | [some-implementations/riverhog/storage/filesystem/src/a\_riverhog\_filesystem\_store/app.py](../../../../../../some-implementations/riverhog/storage/filesystem/src/a_riverhog_filesystem_store/app.py) | `os.getenv(variable, '')` |

### Machine authority

- `/external_contract/configuration_environment/53`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e618274c14181da7c21aa388678df7967d03e3f2da813a12ef63c428a8743f0c -->

```json
{
  "consumers": [
    "a-riverhog-filesystem-store"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-filesystem-store:environment:A_RIVERHOG_FILESYSTEM_STORE_ROOT",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_FILESYSTEM_STORE_ROOT",
  "owner": "a-riverhog-filesystem-store"
}
```

</details>
