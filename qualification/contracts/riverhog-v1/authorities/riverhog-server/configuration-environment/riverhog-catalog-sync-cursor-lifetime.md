# RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-catalog-sync-cursor-lifetime:635b93e8a7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-ef146f919d"></a>

| Field | Value |
|---|---|
| <a id="s-93a23b729e"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-4a3b51c44c"></a>`default_expressions` | `["'24h'"]` |
| <a id="s-7df70bf7e9"></a>`id` | `"riverhog-server:environment:RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME"` |
| <a id="s-202888fb55"></a>`input_shape` | `"environment-string"` |
| <a id="s-4e7ddbc9fe"></a>`name` | `"RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME"` |
| <a id="s-3e6453e97a"></a>`owner` | `"riverhog-server"` |

## Governing policies

- <a id="pa-cc49c039c0"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME](../../../evidence/sources/authorities.md#src-afa71403d4) — [riverhog/src/riverhog\_core/runtime\_config.py::load\_runtime\_config](../../../../../../riverhog/src/riverhog_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/runtime\_config.py](../../../../../../riverhog/src/riverhog_core/runtime_config.py) | `os.getenv('RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME', '24h')` |

### Machine authority

- `/external_contract/configuration_environment/52`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 72c266eb336e70afa2436ce48d879e5d129ea88a9e1a1eb3841a56236a27dd24 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "'24h'"
  ],
  "id": "riverhog-server:environment:RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME",
  "input_shape": "environment-string",
  "name": "RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME",
  "owner": "riverhog-server"
}
```

</details>
