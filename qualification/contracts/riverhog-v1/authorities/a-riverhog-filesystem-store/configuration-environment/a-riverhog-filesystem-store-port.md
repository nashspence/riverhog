# A_RIVERHOG_FILESYSTEM_STORE_PORT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-filesystem-store:a-riverhog-filesystem-store-port:fc990b8f5d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-filesystem-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-9857458ffa"></a>

| Field | Value |
|---|---|
| <a id="s-fe693377ae"></a>`consumers` | `["a-riverhog-filesystem-store"]` |
| <a id="s-0473aebcab"></a>`default_expressions` | `["'8080'"]` |
| <a id="s-8cbdf403cb"></a>`id` | `"a-riverhog-filesystem-store:environment:A_RIVERHOG_FILESYSTEM_STORE_PORT"` |
| <a id="s-7b2e5c71b6"></a>`input_shape` | `"environment-string"` |
| <a id="s-03c589148c"></a>`name` | `"A_RIVERHOG_FILESYSTEM_STORE_PORT"` |
| <a id="s-0666993cd6"></a>`owner` | `"a-riverhog-filesystem-store"` |

## Governing policies

- <a id="pa-2706af91f8"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

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

- `/external_contract/configuration_environment/51`

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
