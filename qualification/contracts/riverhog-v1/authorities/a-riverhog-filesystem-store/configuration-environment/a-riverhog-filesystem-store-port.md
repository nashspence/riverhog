# A_RIVERHOG_FILESYSTEM_STORE_PORT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-filesystem-store:a-riverhog-filesystem-store-port:b67de8699c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-filesystem-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-5f56b9ebf5"></a>

| Field | Value |
|---|---|
| <a id="s-451b5d705f"></a>`consumers` | `["a-riverhog-filesystem-store"]` |
| <a id="s-f6a8f3b6c7"></a>`default_expressions` | `["'8080'"]` |
| <a id="s-1b1b7d2c5f"></a>`id` | `"a-riverhog-filesystem-store:environment:A_RIVERHOG_FILESYSTEM_STORE_PORT"` |
| <a id="s-43982f1d97"></a>`input_shape` | `"environment-string"` |
| <a id="s-7dbd023a7b"></a>`name` | `"A_RIVERHOG_FILESYSTEM_STORE_PORT"` |
| <a id="s-d0a5790f47"></a>`owner` | `"a-riverhog-filesystem-store"` |

## Governing policies

- <a id="pa-aa1ee6bd24"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-filesystem-store:A_RIVERHOG_FILESYSTEM_STORE_PORT](../../../evidence/sources/authorities.md#src-ba32f13bc6) — [some-implementations/riverhog/storage/filesystem/src/a\_riverhog\_filesystem\_store/app.py::\_parser](../../../../../../some-implementations/riverhog/storage/filesystem/src/a_riverhog_filesystem_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-filesystem-store` | [some-implementations/riverhog/storage/filesystem/src/a\_riverhog\_filesystem\_store/app.py](../../../../../../some-implementations/riverhog/storage/filesystem/src/a_riverhog_filesystem_store/app.py) | `os.getenv(f'{_PREFIX}PORT', '8080')` |

### Machine authority

- `/external_contract/configuration_environment/105`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 63ce4ea3b0ca1699720a773fe1cf6fd4e74d4b1468e1a0949430c70f02978cf2 -->

```json
{
  "consumers": [
    "a-riverhog-filesystem-store"
  ],
  "default_expressions": [
    "'8080'"
  ],
  "id": "a-riverhog-filesystem-store:environment:A_RIVERHOG_FILESYSTEM_STORE_PORT",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_FILESYSTEM_STORE_PORT",
  "owner": "a-riverhog-filesystem-store"
}
```

</details>
